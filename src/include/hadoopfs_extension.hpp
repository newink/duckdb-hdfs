#pragma once

#include "duckdb.hpp"
#include "hadoopfs.hpp"
#include "easylogging++.h"

namespace duckdb {

class HadoopfsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb