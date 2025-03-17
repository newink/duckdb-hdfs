#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "hadoopfs_extension.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb
{
    static std::mutex mutex;

    static bool isLoaded(DatabaseInstance &instance)
    {
        auto &fs = instance.GetFileSystem();
        auto sub_system_list = fs.ListSubSystems();
        auto it = std::find(sub_system_list.begin(), sub_system_list.end(), "HadoopFileSystem");
        if (DUCKDB_LIKELY(it != sub_system_list.end()))
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    static void LoadInternal(DatabaseInstance &instance)
    {
        if (DUCKDB_UNLIKELY(!isLoaded(instance)))
        {
            mutex.lock();
            if (DUCKDB_UNLIKELY(!isLoaded(instance)))
            {
                auto &fs = instance.GetFileSystem();
                fs.RegisterSubSystem(duckdb::make_uniq<HadoopFileSystem>(instance));
            }
            mutex.unlock();
        }
    }

    void HadoopfsExtension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }

    std::string HadoopfsExtension::Name()
    {
        return "hadoopfs";
    }

} // namespace duckdb

extern "C"
{

    DUCKDB_EXTENSION_API void hadoopfs_init(duckdb::DatabaseInstance &db)
    {
        LoadInternal(db);
    }

    DUCKDB_EXTENSION_API const char *hadoopfs_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
