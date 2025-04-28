#include "hadoopfs.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/scalar/string_common.hpp"

#include "hadoopfs_extension.hpp"
#include "easylogging++.h"

#include <chrono>
#include <string>
#include <shared_mutex>
#include <thread>
#include <map>
#include <atomic>
#include <vector>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <limits.h>

namespace duckdb
{
    static std::mutex local_ip_mutex;

    static string getIPAddress(const string & hostname) 
    {
        struct addrinfo hints, *res;
        int status;
        char ipstr[INET6_ADDRSTRLEN];

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC; // AF_INET or AF_INET6 to force version
        hints.ai_socktype = SOCK_STREAM;

        if ((status = getaddrinfo(hostname.c_str(), nullptr, &hints, &res)) != 0) 
        {
            //std::cerr << "getaddrinfo: " << gai_strerror(status) << std::endl;
            return "";
        }

        for(struct addrinfo *p = res; p != nullptr; p = p->ai_next) 
        {
            void *addr;

            if (p->ai_family == AF_INET) { // IPv4
                struct sockaddr_in *ipv4 = (struct sockaddr_in *)p->ai_addr;
                addr = &(ipv4->sin_addr);
            } else { // IPv6
                struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)p->ai_addr;
                addr = &(ipv6->sin6_addr);
            }

            inet_ntop(p->ai_family, addr, ipstr, sizeof(ipstr));
            freeaddrinfo(res);
            return string(ipstr);
        }

        freeaddrinfo(res);
        return "";
    }

    inline static std::vector<string> getLocalIPAddresses() 
    {
        static std::vector<string> localIPs;
        if (!localIPs.empty()) return localIPs;
        std::lock_guard<std::mutex> lock(local_ip_mutex);
        if (!localIPs.empty()) return localIPs;
        //LOG(DEBUG) << "getLocalIPAddresses...";
        struct ifaddrs *ifaddr, *ifa;
        char ip[INET6_ADDRSTRLEN];

        if (getifaddrs(&ifaddr) == -1) 
        {
            //perror("getifaddrs");
            return localIPs;
        }

        for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) 
        {
            if (ifa->ifa_addr == nullptr) continue;

            int family = ifa->ifa_addr->sa_family;

            if (family == AF_INET) 
            { // IPv4
                struct sockaddr_in *addr = (struct sockaddr_in *)ifa->ifa_addr;
                inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
                localIPs.push_back(ip);
            } 
            else if (family == AF_INET6) 
            { // IPv6
                struct sockaddr_in6 *addr = (struct sockaddr_in6 *)ifa->ifa_addr;
                inet_ntop(AF_INET6, &addr->sin6_addr, ip, sizeof(ip));
                localIPs.push_back(ip);
            }
        }
        freeifaddrs(ifaddr);
        return localIPs;
    }

    inline static string getUrlHost(const string & url)
    {
        if (StringUtil::Lower(url).rfind("hdfs://", 0) != 0)
        {
            throw IOException("URL needs to start with hdfs://");
        }
        string host;
        auto slash_pos = url.find('/', 8);
        if (slash_pos == string::npos)
        {
            host = url.substr(7);
        } else {
            host = url.substr(7, slash_pos);
        }
        size_t portSymboPos = host.rfind(':');
        if (portSymboPos != string::npos) {
            host = host.substr(0, portSymboPos);
        }
        return host;
    }
    
    inline static bool isEqualNameNode(const string & firstUrl, const string & secondUrl) 
    {
        if (firstUrl.empty() || secondUrl.empty()) return false;

        string firstHost = getUrlHost(firstUrl);
        firstHost = StringUtil::Lower(firstHost);

        string secondHost = getUrlHost(secondUrl);
        secondHost = StringUtil::Lower(secondHost);

        if(StringUtil::Contains(firstUrl, secondUrl) || 
            StringUtil::Contains(secondUrl, firstUrl))
        {
            return true;
        }

        string firstIp = getIPAddress(firstHost);
        LOG(TRACE) << "[" << firstHost << "] ip [" << firstIp << "]";
        string secondIp = getIPAddress(secondHost);
        LOG(TRACE) << "[" << secondHost << "] ip [" << secondIp << "]";

        if (!firstIp.empty() && !secondIp.empty()) {
            if (firstIp == secondIp)
            {
                LOG(TRACE) << "[" << firstHost << "] ip equal to [" << secondHost << "]";
                return true;
            }
            else
            {
                std::vector<string> localIPs = getLocalIPAddresses();
                bool isFirstHostLocal = 
                    std::find(localIPs.begin(), localIPs.end(), firstIp) != localIPs.end();
                bool isSecondHostLocal = 
                    std::find(localIPs.begin(), localIPs.end(), secondIp) != localIPs.end();
                if (isFirstHostLocal && isSecondHostLocal) 
                {
                    LOG(TRACE) << "[" << firstHost << ", " << secondHost << "] is localhost";
                    return true;
                }
            }
        }

        LOG(TRACE) << "[" << firstHost << "] not equal to [" << secondHost << "]";
        return false;
    }

    inline static void setHdfsBuilderParams(hdfsBuilder * hdfsBuilder, const HDFSParams & params) {

        hdfsBuilderSetNameNode(hdfsBuilder, "default");
        if (params.namenode != "default")
        {
            if (params.url.empty())
            {
                string defaultUri = "hdfs://" + params.namenode;
                hdfsBuilderConfSetStr(hdfsBuilder, "dfs.default.uri", defaultUri.c_str());
            }
            hdfsBuilderSetNameNode(hdfsBuilder, params.namenode.c_str());
        }

        if (!params.ha_namenodes.empty())
        {
            string cluster = "duckdb";
            LOG(TRACE) << "dfs.nameservices: " << cluster;
            hdfsBuilderConfSetStr(hdfsBuilder, "dfs.nameservices", cluster.c_str());
            string haNamenodesKey = "dfs.ha.namenodes." + cluster;
            std::stringstream ssNamenodes;
            for(size_t index=0; index < params.ha_namenodes.size(); index++)
            {
                string namenodeName =  "nn" + to_string(index);
                if (index > 0) ssNamenodes << ",";
                ssNamenodes << namenodeName;
                string namenodeRpcKey = "dfs.namenode.rpc-address." + cluster + "." + namenodeName;
                LOG(TRACE) << namenodeRpcKey <<": " << params.ha_namenodes[index];
                hdfsBuilderConfSetStr(hdfsBuilder, namenodeRpcKey.c_str(), params.ha_namenodes[index].c_str());
            }
            string namenodes = ssNamenodes.str();
            LOG(TRACE) << haNamenodesKey <<": " << namenodes;
            hdfsBuilderConfSetStr(hdfsBuilder, haNamenodesKey.c_str(), namenodes.c_str());
        }
        
        if (!params.shortcircuit || params.domain_socket_path.empty())
        {
            hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "false");
        }
        else
        {
            hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "true");
            hdfsBuilderConfSetStr(hdfsBuilder, "dfs.domain.socket.path",
                                  params.domain_socket_path.c_str());
        }
    }

    static void reconfigureLogger(const HDFSLogParams &hdfs_log_params)
    {
        el::Configurations defaultConf;
        defaultConf.setToDefault();
        defaultConf.setGlobally(el::ConfigurationType::Enabled, "false");
        defaultConf.setGlobally(el::ConfigurationType::ToFile, "false");
        el::Level level = el::LevelHelper::convertFromString(hdfs_log_params.log_level.c_str());
        switch (level)
        {
        case el::Level::Trace:
            defaultConf.set(el::Level::Trace, el::ConfigurationType::Enabled, "true");
        case el::Level::Debug:
            defaultConf.set(el::Level::Debug, el::ConfigurationType::Enabled, "true");
        case el::Level::Info:
            defaultConf.set(el::Level::Info, el::ConfigurationType::Enabled, "true");
        case el::Level::Warning:
            defaultConf.set(el::Level::Warning, el::ConfigurationType::Enabled, "true");
        case el::Level::Error:
            defaultConf.set(el::Level::Error, el::ConfigurationType::Enabled, "true");
        case el::Level::Fatal:
            defaultConf.set(el::Level::Fatal, el::ConfigurationType::Enabled, "true");
            if (!hdfs_log_params.log_file.empty() &&
                !std::all_of(hdfs_log_params.log_file.begin(), hdfs_log_params.log_file.end(), ::isspace))
            {
                defaultConf.setGlobally(el::ConfigurationType::ToFile, "true");
                defaultConf.setGlobally(el::ConfigurationType::Filename, hdfs_log_params.log_file);
                defaultConf.setGlobally(el::ConfigurationType::MaxLogFileSize, std::to_string(1 << 30));
            }
        default:
            defaultConf.setGlobally(el::ConfigurationType::ToStandardOutput, "false");
            defaultConf.setGlobally(el::ConfigurationType::Format, "[%datetime{%Y-%M-%d %h:%m:%s.%g}][%level][%func][%msg]");
            break;
        }
        el::Loggers::reconfigureLogger("default", defaultConf);
        LOG(INFO) << "Reconfigured logger. Level: " << hdfs_log_params.log_level << ", File: " << hdfs_log_params.log_file;
    }

    static void hdfs_reconfigure_logger_pragma_function(ClientContext &context, const FunctionParameters &parameters)
    {
        HDFSLogParams hdfs_log_params = HDFSLogParams::ReadFrom(context);
        reconfigureLogger(hdfs_log_params);
    }

    HadoopFileHandle::HadoopFileHandle(FileSystem &fs, string path, FileOpenFlags flags, hdfsFS hdfs)
        : FileHandle(fs, path, flags), hdfs(hdfs), flags(flags), length(0)
    {
    }

    void HadoopFileHandle::Initialize(FileOpener *opener)
    {
    }

    HadoopFileHandle::~HadoopFileHandle()
    {
        Close();
    }

    void HadoopFileHandle::Close()
    {
        if (hdfs_file)
        {
            LOG(TRACE) << "file: " << path;
            hdfsCloseFile(hdfs, hdfs_file);
            hdfs_file = nullptr;
        }
    }

    void HadoopFileSystem::ParseUrl(const string &url, string &path_out, string &proto_host_port_out)
    {
        if (StringUtil::Lower(url).rfind("hdfs://", 0) != 0)
        {
            throw IOException("URL needs to start with hdfs://");
        }
        auto slash_pos = url.find('/', 8);
        if (slash_pos == string::npos)
        {
            throw IOException("URL needs to contain a '/' after the host");
        }
        proto_host_port_out = url.substr(0, slash_pos);
        path_out = url.substr(slash_pos);
        if (path_out.empty())
        {
            throw IOException("URL needs to contain a path");
        }
    }

    void HadoopFileSystem::ParseUrl(const string &url, string &path_out, HDFSParams &hdfs_params_out)
    {
        if (StringUtil::Lower(url).rfind("hdfs://", 0) != 0)
        {
            throw IOException("URL needs to start with hdfs://");
        }
        hdfs_params_out.url = url;
        auto slash_pos = url.find('/', 8);
        if (slash_pos == string::npos)
        {
            path_out = "/";
            hdfs_params_out.namenode = url.substr(7);
        }
        else
        {
            path_out = url.substr(slash_pos);
            hdfs_params_out.namenode = url.substr(7, slash_pos - 7);
            if (hdfs_params_out.namenode.empty())
            {
                hdfs_params_out.namenode = "default";
            }
        }
    }

    bool HadoopFileSystem::Match(FileType file_type,
                                 vector<string>::const_iterator key, vector<string>::const_iterator key_end,
                                 vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end)
    {

        while (key != key_end && pattern != pattern_end)
        {
            if (*pattern == "**")
            {
                if (file_type == FileType::FILE_TYPE_DIR)
                {
                    return true;
                }
                if (std::next(pattern) == pattern_end)
                {
                    return true;
                }
                while (key != key_end)
                {
                    if (Match(file_type, key, key_end, std::next(pattern), pattern_end))
                    {
                        return true;
                    }
                    key++;
                }
                return false;
            }
            if (!duckdb::Glob(key->data(), key->length(), pattern->data(), pattern->length(), true))
            {
                return false;
            }
            key++;
            pattern++;
        }
        return key == key_end && pattern == pattern_end;
    }

    bool HadoopFileSystem::ListFiles(const string &directory,
                                     const std::function<void(const string &, bool)> &callback,
                                     FileOpener *opener)
    {
        string path_out, proto_host_port;
        HadoopFileSystem::ParseUrl(directory, path_out, proto_host_port);
        LOG(TRACE) << "directory: " << directory;
        LOG(TRACE) << "proto_host_port: " << proto_host_port;
        LOG(TRACE) << "path_out: " << path_out;
        int num_entries;
        auto hdfsFS = GetHadoopFileSystem(directory, opener);
        LOG(TRACE) << "hdfsFS: " << hdfsFS;
        hdfsFileInfo *file_info = hdfsListDirectory(hdfsFS, path_out.c_str(), &num_entries);
        if (file_info == nullptr)
        {
            return false;
        }

        for (int i = 0; i < num_entries; ++i)
        {
            string file_path = proto_host_port;
            string proto_host_port_with_separator = PathSeparator(proto_host_port);
            LOG(TRACE) << "proto_host_port_with_separator: " << proto_host_port_with_separator << ", file: " << file_info[i].mName;
            if (StringUtil::StartsWith(file_info[i].mName, proto_host_port_with_separator))
            {
                file_path += file_info[i].mName;
            }
            else
            {
                file_path += proto_host_port_with_separator + file_info[i].mName;
            }
            callback(file_path, file_info[i].mKind == kObjectKindDirectory);
        }

        hdfsFreeFileInfo(file_info, num_entries);

        return true;
    }

    vector<string> HadoopFileSystem::Glob(const string &glob_pattern, FileOpener *opener)
    {
        auto glob_start = std::chrono::high_resolution_clock::now();
        if (opener == nullptr)
        {
            throw InternalException("Cannot HDFS Glob without FileOpener");
        }

        LOG(TRACE) << "glob_pattern: " << glob_pattern;

        vector<std::string> output;
        auto callback = [&](const string &_glob_pattern, vector<string> &_output)
        {
            LOG(TRACE) << "Get glob from HDFS.";
            FileOpenerInfo info = {_glob_pattern};

            // matches on prefix, not glob pattern, so we take a substring until the first wildcard char
            auto first_wildcard_pos = _glob_pattern.find_first_of("*[\\");
            // LOG(TRACE) << "first_wildcard_pos: " << first_wildcard_pos;
            if (first_wildcard_pos == string::npos)
            {
                _output.emplace_back(_glob_pattern);
                return;
            }

            auto first_slash_pos = _glob_pattern.find('/', 7);
            // LOG(TRACE) << "first_slash_pos: " << first_slash_pos;
            if (first_slash_pos == string::npos)
            {
                _output.emplace_back(_glob_pattern);
                return;
            }

            auto first_slash_before_wildcard = _glob_pattern.rfind('/', first_wildcard_pos);
            // LOG(TRACE) << "first_slash_before_wildcard: " << first_slash_before_wildcard;
            if (first_slash_before_wildcard == string::npos)
            {
                _output.emplace_back(_glob_pattern);
                return;
            }

            string shared_path = _glob_pattern.substr(0, first_slash_before_wildcard);
            string shared_pattern = _glob_pattern.substr(first_slash_before_wildcard + 1);

            // LOG(TRACE) << "shared_path: " << shared_path;
            // LOG(TRACE) << "shared_pattern: " << shared_pattern;

            auto pattern_list = StringUtil::Split(shared_pattern, "/");
            vector<string> path_list;
            path_list.emplace_back(shared_path);
            while (!path_list.empty())
            {
                string current_path = path_list.back();
                path_list.pop_back();
                // LOG(TRACE) << "current_path: " << current_path;
                //  Printer::Print("Current path: " + current_path);
                ListFiles(
                    current_path,
                    [&](const string &fname, bool is_directory)
                    {
                        auto match_path_list = StringUtil::Split(fname.substr(first_slash_before_wildcard + 1), "/");
                        if (is_directory && Match(FileType::FILE_TYPE_DIR, match_path_list.begin(), match_path_list.end(),
                                                  pattern_list.begin(), pattern_list.begin() + match_path_list.size()))
                        {
                            // LOG(TRACE) << "Push dir: " << fname;
                            path_list.emplace_back(fname);
                        }
                        else if (Match(FileType::FILE_TYPE_REGULAR, match_path_list.begin(), match_path_list.end(),
                                       pattern_list.begin(), pattern_list.end()))
                        {
                            LOG(TRACE) << "Match file: " << fname;
                            _output.emplace_back(fname);
                        }
                        else
                        {
                            // LOG(TRACE) << "Skip file: " << fname;
                            //  throw IOException("Invalid file name: " + fname);
                        }
                    },
                    opener);
            }
            if (!_output.empty())
            {
                std::sort(_output.begin(), _output.end());
            }
        };

        callback(glob_pattern, output);

        auto glob_end = std::chrono::high_resolution_clock::now();
        auto glob_duration = std::chrono::duration_cast<std::chrono::microseconds>(glob_end - glob_start);
        LOG(TRACE) << "glob_duration: " << glob_duration.count() << "μs";
        return output;
    }

    HadoopFileSystem::HadoopFileSystem(DatabaseInstance &instance) : instance(instance)
    {

        auto &config = DBConfig::GetConfig(instance);

        // Global HDFS config
        config.AddExtensionOption(HDFSParams::HDFS_DEFAULT_NAMENODE, "HDFS default namenode", LogicalType::VARCHAR, "default");
        config.AddExtensionOption(HDFSParams::HDFS_HA_NAMENODES, "HDFS HA namenodes", LogicalType::VARCHAR, "");
        config.AddExtensionOption(HDFSParams::HDFS_SHORTCIRCUIT, "HDFS short-circuit", LogicalType::BOOLEAN, false);
        config.AddExtensionOption(HDFSParams::HDFS_DOMAIN_SOCKET_PATH, "HDFS domain socket path", LogicalType::VARCHAR, "");

        config.AddExtensionOption(HDFSLogParams::HDFS_LOG_LEVEL, "HDFS log level", LogicalType::VARCHAR, "INFO");
        #ifdef ELPP_DEFAULT_LOG_FILE
        config.AddExtensionOption(HDFSLogParams::HDFS_LOG_FILE, "HDFS log file", LogicalType::VARCHAR, ELPP_DEFAULT_LOG_FILE);
        #else
        config.AddExtensionOption(HDFSLogParams::HDFS_LOG_FILE, "HDFS log file", LogicalType::VARCHAR, "hadoopfs.extension.log");
        #endif

        auto provider = duckdb::make_uniq<HDFSEnvironmentSettingsProvider>(config);
        provider->SetAll();
        HDFSLogParams hdfs_log_params = HDFSLogParams::ReadFrom(instance);
        reconfigureLogger(hdfs_log_params);

        PragmaFunction hdfs_reconfigure_logger =
            PragmaFunction::PragmaStatement("hdfs_reconf_logger", hdfs_reconfigure_logger_pragma_function);

        ExtensionUtil::RegisterFunction(instance, hdfs_reconfigure_logger);

        LOG(INFO) << "Init Hadoop File System.";
        LOG(INFO) << "Platform: " << DuckDB::Platform();
        LOG(INFO) << "Version: " << DuckDB::LibraryVersion();
        LOG(INFO) << "Source ID: " << DuckDB::SourceID();
#if defined(HADOOPFS_VERSION)
        LOG(INFO) << "Extension Version: " << HADOOPFS_VERSION;
#endif
    }

    HadoopFileSystem::~HadoopFileSystem()
    {
        LOG(INFO) << "Destroy Hadoop File System.";
        std::unique_lock<std::shared_mutex> writeLock(hdfs_mutex);
        for (const auto &pair : hdfs_map)
        {
            hdfsDisconnect(pair.second);
        }
    }

    hdfsFS HadoopFileSystem::GetHadoopFileSystemFromMap(const string key)
    {
        std::shared_lock<std::shared_mutex> readLock(hdfs_mutex);
        auto it = hdfs_map.find(key);
        if (it != hdfs_map.end())
        {
            return it->second;
        }
        return nullptr;
    }

    hdfsFS HadoopFileSystem::CreateAndPutHadoopFileSystemToMap(const string key, 
                                                               const std::function<hdfsFS()> & get_hdfs_fs)
    {
        std::unique_lock<std::shared_mutex> writeLock(hdfs_mutex);
        hdfsFS hdfs = nullptr;
        auto it = hdfs_map.find(key);
        if (it != hdfs_map.end())
        {
            hdfs = it->second;
        }
        if (hdfs == nullptr)
        {
            hdfs = get_hdfs_fs();
        }
        if (hdfs != nullptr)
        {
            hdfs_map[key] = hdfs;
        }
        return hdfs;
    }

    hdfsFS HadoopFileSystem::GetHadoopFileSystem(const string key, 
                                                 const std::function<hdfsFS()> & get_hdfs_fs)
    {
        hdfsFS hdfs = GetHadoopFileSystemFromMap(key);
        if (hdfs == nullptr)
        {
            hdfs = CreateAndPutHadoopFileSystemToMap(key, get_hdfs_fs);
        }
        return hdfs;
    }

    hdfsFS HadoopFileSystem::GetHadoopFileSystem()
    {
        HDFSParams hdfs_params = HDFSParams::ReadFrom(instance);
        hdfsFS hdfs = GetHadoopFileSystem(hdfs_params.namenode, [&]()
                                          { return GetHadoopFileSystem(hdfs_params); });
        return hdfs;
    }

    hdfsFS HadoopFileSystem::GetHadoopFileSystemWithException()
    {
        auto hdfs = GetHadoopFileSystem();
        if (!hdfs)
        {
            HDFSParams hdfs_params = HDFSParams::ReadFrom(instance);
            throw IOException("Unable to connect to HDFS: " + hdfs_params.namenode);
        }
        return hdfs;
    }

    hdfsFS HadoopFileSystem::GetHadoopFileSystem(const string &url, FileOpener *opener)
    {
        string url_path;
        HDFSParams url_hdfs_params, opener_hdfs_params, instance_hdfs_params;

        if (url.empty())
        {
            return GetHadoopFileSystem();
        }

        HadoopFileSystem::ParseUrl(url, url_path, url_hdfs_params);
        if (url_hdfs_params.namenode == "default")
        {
            return GetHadoopFileSystem();
        }

        auto create_hdfs = [&]()
        {
            LOG(TRACE) << "Create HDFS: " << url_hdfs_params.namenode;
            if (opener)
            {
                ClientContext *context = opener->TryGetClientContext().get();
                opener_hdfs_params = HDFSParams::ReadFrom(*context);
            }

            instance_hdfs_params = HDFSParams::ReadFrom(instance);
            auto hdfs_builder = hdfsNewBuilder();
            setHdfsBuilderParams(hdfs_builder, url_hdfs_params);

            LOG(TRACE) << "URL_DEFAULT_NAME_NODE: " << url_hdfs_params.namenode;
            LOG(TRACE) << "OPENER_DEFAULT_NAME_NODE: " << opener_hdfs_params.namenode;
            LOG(TRACE) << "INSTANCE_DEFAULT_NAME_NODE: " << instance_hdfs_params.namenode;

            return hdfsBuilderConnect(hdfs_builder);
        };

        hdfsFS hdfs = GetHadoopFileSystem(url_hdfs_params.namenode, create_hdfs);
        return hdfs;
    }

    hdfsFS HadoopFileSystem::GetHadoopFileSystem(const HDFSParams &hdfs_params)
    {
        LOG(TRACE) << "Create HDFS: " << hdfs_params.namenode;
        auto hdfs_builder = hdfsNewBuilder();

        setHdfsBuilderParams(hdfs_builder, hdfs_params);

        auto fs = hdfsBuilderConnect(hdfs_builder);
        return fs;
    }

    unique_ptr<HadoopFileHandle> HadoopFileSystem::CreateHandle(const string &path, 
                                                                FileOpenFlags flags, 
                                                                optional_ptr<FileOpener> opener)
    {
        D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);
        string path_out, proto_host_port;
        HadoopFileSystem::ParseUrl(path, path_out, proto_host_port);
        LOG(TRACE) << "Path: " << path;
        LOG(TRACE) << "Path Out: " << path_out;
        LOG(TRACE) << "Proto Host Port: " << proto_host_port;
        hdfsFS fs = GetHadoopFileSystem(path, opener.get());
        if (!fs)
        {
            throw IOException("Unable to connect to HDFS: " + proto_host_port);
        }

        auto hadoop_file_handle =
            duckdb::make_uniq<HadoopFileHandle>(*this, path, flags, fs);

        hdfsFileInfo *file_info = hdfsGetPathInfo(hadoop_file_handle->hdfs, path_out.c_str());
        if (!file_info)
        {
            if (hadoop_file_handle->flags.OpenForWriting())
            {
                auto last_slash_pos = path_out.rfind('/');
                if (last_slash_pos == string::npos)
                {
                    throw IOException("Unable to get file dir: " + path);
                }
                auto file_dir = path_out.substr(0, last_slash_pos);
                file_info = hdfsGetPathInfo(hadoop_file_handle->hdfs, file_dir.c_str());
                if (!file_info)
                {
                    hdfsCreateDirectory(hadoop_file_handle->hdfs, file_dir.c_str());
                }
                else
                {
                    hdfsFreeFileInfo(file_info, 1);
                }
            }
            else
            {
                Printer::Print(hdfsGetLastError());
                throw IOException("Unable to get file info: " + path);
            }
        }
        else
        {
            if (file_info->mKind == kObjectKindDirectory)
            {
                hadoop_file_handle->file_type = FileType::FILE_TYPE_DIR;
            }
            else if (file_info->mKind == kObjectKindFile)
            {
                hadoop_file_handle->file_type = FileType::FILE_TYPE_REGULAR;
            }
            else
            {
                hadoop_file_handle->file_type = FileType::FILE_TYPE_INVALID;
            }
            hadoop_file_handle->length = file_info->mSize;
            hadoop_file_handle->last_modified = file_info->mLastMod;
            hdfsFreeFileInfo(file_info, 1);
        }

        int hdfs_flag = 0;
        if (flags.OpenForWriting() &&
            (flags.OpenForWriting() || flags.OpenForAppending()))
        {
            hdfs_flag |= O_RDWR;
        }
        else if (flags.OpenForReading())
        {
            hdfs_flag |= O_RDONLY;
        }
        else if ((flags.OpenForWriting() || flags.OpenForAppending()))
        {
            hdfs_flag |= O_WRONLY;
        }
        hadoop_file_handle->hdfs_file =
            hdfsOpenFile(hadoop_file_handle->hdfs, path_out.c_str(), hdfs_flag, 0, 0, 0);
        if (!hadoop_file_handle->hdfs_file)
        {
            LOG(ERROR) << "Failed to open file: " << hdfsGetLastError();
            throw IOException("Failed to open file: %s", hdfsGetLastError());
        }
        return hadoop_file_handle;
    }

    FileType HadoopFileSystem::GetFileType(FileHandle &handle)
    {
        auto &hfh = (HadoopFileHandle &)handle;
        return hfh.file_type;
    }

    unique_ptr<FileHandle> HadoopFileSystem::OpenFile(const string &path, 
                                                      FileOpenFlags flags,
	                                                  optional_ptr<FileOpener> opener)
    {
        auto handle = CreateHandle(path, flags, opener);
        return unique_ptr<FileHandle>(std::move(handle));
    }

    void HadoopFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location)
    {
        ReadFromHDFS(handle, buffer, nr_bytes, location);
    }

    int64_t HadoopFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes)
    {
        return ReadFromHDFS(handle, buffer, nr_bytes);
    }

    int64_t HadoopFileSystem::ReadFromHDFS(FileHandle &handle, void *buffer, int64_t nr_bytes)
    {
        auto &hfh = (HadoopFileHandle &)handle;
        auto length = hdfsRead(hfh.hdfs, hfh.hdfs_file, buffer, nr_bytes);
        return length;
    }

    int64_t HadoopFileSystem::ReadFromHDFS(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location)
    {
        auto &hfh = (HadoopFileHandle &)handle;
        Seek(handle, location);
        auto read_byte_count = 0;
        while (read_byte_count < nr_bytes)
        {
            void *offset_buffer = static_cast<char *>(buffer) + read_byte_count;
            auto length = ReadFromHDFS(handle, offset_buffer, nr_bytes - read_byte_count);
            LOG(TRACE) << "file: " << handle.path;
            LOG(TRACE) << "location: " << location + read_byte_count;
            LOG(TRACE) << "nr_bytes: " << nr_bytes - read_byte_count;
            LOG(TRACE) << "length: " << length;
            if (length > 0)
            {
                read_byte_count += length;
            }
            else
            {
                if (length < 0)
                {
                    LOG(ERROR) << "Failed to read file: " << hdfsGetLastError();
                }
                break;
            }
        }
        LOG(TRACE) << "total read byte count: " << read_byte_count;
        return read_byte_count;
    }

    void HadoopFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location)
    {
        auto offset = location - SeekPosition(handle);
        Seek(handle, offset);
        auto write_byte_count = 0;
        while (write_byte_count < nr_bytes)
        {
            void *offset_buffer = static_cast<char *>(buffer) + write_byte_count;
            auto length = Write(handle, offset_buffer, nr_bytes - write_byte_count);
            LOG(TRACE) << "file: " << handle.path;
            LOG(TRACE) << "location: " << location + write_byte_count;
            LOG(TRACE) << "nr_bytes: " << nr_bytes - write_byte_count;
            LOG(TRACE) << "length: " << length;
            if (length >= 0)
            {
                write_byte_count += length;
            }
            else
            {
                LOG(ERROR) << "Failed to write file: " << hdfsGetLastError();
                break;
            }
        }
    }

    int64_t HadoopFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes)
    {
        LOG(TRACE) << "file: " << handle.path;
        LOG(TRACE) << "nr_bytes: " << nr_bytes;
        auto &hfh = (HadoopFileHandle &)handle;
        if (!hfh.flags.OpenForWriting())
        {
            throw InternalException("Write called on file not opened in write mode");
        }
        return hdfsWrite(hfh.hdfs, hfh.hdfs_file, buffer, nr_bytes);
    }

    void HadoopFileSystem::FileSync(FileHandle &handle)
    {
        auto &hfh = (HadoopFileHandle &)handle;
        hdfsSync(hfh.hdfs, hfh.hdfs_file);
    }

    int64_t HadoopFileSystem::GetFileSize(FileHandle &handle)
    {
        auto &sfh = (HadoopFileHandle &)handle;
        return sfh.length;
    }

    time_t HadoopFileSystem::GetLastModifiedTime(FileHandle &handle)
    {
        auto &hfh = (HadoopFileHandle &)handle;
        return hfh.last_modified;
    }

    bool HadoopFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener)
    {
        try
        {
            auto hdfs = GetHadoopFileSystem(filename);
            if (hdfsExists(hdfs, filename.c_str()) == 0)
            {
                return true;
            }
            return false;
        }
        catch (...)
        {
            return false;
        };
    }

    bool HadoopFileSystem::CanHandleFile(const string &fpath)
    {
        return StringUtil::Lower(fpath).rfind("hdfs://", 0) == 0;
    }

    void HadoopFileSystem::Seek(FileHandle &handle, idx_t location)
    {
        auto &hfh = (HadoopFileHandle &)handle;
        auto position = SeekPosition(handle);
        int64_t offset = location - position;
        if (offset == 0)
        {
            LOG(TRACE) << "Skip! Seek Path: " << hfh.path << ", Handle: " << hfh.hdfs_file << ", Position: " << position << ", Location: " << location << ", Offset: " << offset;
        }
        else
        {
            LOG(TRACE) << "Seek Path: " << hfh.path << ", Handle: " << hfh.hdfs_file << ", Position: " << position << ", Location: " << location << ", Offset: " << offset;
            if (hdfsSeek(hfh.hdfs, hfh.hdfs_file, location) == -1)
            {
                LOG(ERROR) << "Failed to seek file: " << hdfsGetLastError();
                throw IOException("Failed to seek file: %s", hdfsGetLastError());
            }
        }
    }

    idx_t HadoopFileSystem::SeekPosition(FileHandle &handle)
    {
        auto &hfh = (HadoopFileHandle &)handle;
        return hdfsTell(hfh.hdfs, hfh.hdfs_file);
    }

    void HadoopFileSystem::Truncate(FileHandle &handle, int64_t new_size)
    {
        auto &hfh = (HadoopFileHandle &)handle;
        int should_wait;
        auto result = hdfsTruncate(hfh.hdfs, hfh.path.c_str(), new_size, &should_wait);
    }

    bool HadoopFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener)
    {
        return FileExists(directory);
    }

    void HadoopFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener)
    {
        auto hdfs = GetHadoopFileSystem(directory);
        hdfsCreateDirectory(hdfs, directory.c_str());
    }

    void HadoopFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener)
    {
        auto hdfs = GetHadoopFileSystem(directory);
        hdfsDelete(hdfs, directory.c_str(), 1);
    }

    void HadoopFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener)
    {
        auto hdfs = GetHadoopFileSystem(source);
        hdfsRename(hdfs, source.c_str(), target.c_str());
    }

    void HadoopFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener)
    {
        auto hdfs = GetHadoopFileSystem(filename);
        hdfsDelete(hdfs, filename.c_str(), 1);
    }

    void HadoopFileSystem::Reset(FileHandle &handle)
    {
        Seek(handle, 0);
    }

} // namespace duckdb
