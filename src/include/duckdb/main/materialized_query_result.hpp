//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/materialized_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

// Base class for polymorphism
struct Base {
    virtual ~Base() {} // Virtual destructor for proper cleanup of derived objects
};

// Derived class for integers
struct IntData : public Base {
    int value;
		int id;
    explicit IntData(int val) : value(val) {id = 13;}
};

struct UIntData : public Base {
    uint32_t value;
		int id;
    explicit UIntData(uint32_t val) : value(val) {id = 30;}
};

struct BigIntData : public Base {
    int64_t value;
		int id;
    explicit BigIntData(int64_t val) : value(val) {id = 14;}
};

struct UBigIntData : public Base {
    uint64_t value;
		int id;
    explicit UBigIntData(uint64_t val) : value(val) {id = 31;}
};

// Derived class for doubles
struct DoubleData : public Base {
    double value;
		int id;
    explicit DoubleData(double val) : value(val) {id = 23;}
};

// Derived class for strings
struct StringData : public Base {
    std::string value;
		int id;
    explicit StringData(std::string val) : value(std::move(val)) {id = 25;}
};

// Derived class for booleans
struct BoolData : public Base {
    bool value;
		int id;
    explicit BoolData(bool val) : value(val) {id = 13;}
};

class ClientContext;

class MaterializedQueryResult : public QueryResult {
public:
	static constexpr const QueryResultType TYPE = QueryResultType::MATERIALIZED_RESULT;

public:
	friend class ClientContext;
	//! Creates a successful query result with the specified names and types
	DUCKDB_API MaterializedQueryResult(StatementType statement_type, StatementProperties properties,
	                                   vector<string> names, unique_ptr<ColumnDataCollection> collection,
	                                   ClientProperties client_properties);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API explicit MaterializedQueryResult(ErrorData error);

public:
	//! Fetches a DataChunk from the query result.
	//! This will consume the result (i.e. the result can only be scanned once with this function)
	DUCKDB_API unique_ptr<DataChunk> Fetch() override;
	DUCKDB_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;
	DUCKDB_API string ToBox(ClientContext &context, const BoxRendererConfig &config) override;

	//! Gets the (index) value of the (column index) column.
	//! Note: this is very slow. Scanning over the underlying collection is much faster.
	DUCKDB_API Value GetValue(idx_t column, idx_t index);

	template <class T>
	T GetValue(idx_t column, idx_t index) {
		auto value = GetValue(column, index);
		return (T)value.GetValue<int64_t>();
	}

	DUCKDB_API idx_t RowCount() const;

	//! Returns a reference to the underlying column data collection
	ColumnDataCollection &Collection();

	//! Takes ownership of the collection, 'collection' is null after this operation
	unique_ptr<ColumnDataCollection> TakeCollection();

	// Function to get all the contents of the column
	DUCKDB_API std::vector<std::vector<unique_ptr<Base>>> getContents();

private:
	unique_ptr<ColumnDataCollection> collection;
	//! Row collection, only created if GetValue is called
	unique_ptr<ColumnDataRowCollection> row_collection;
	//! Scan state for Fetch calls
	ColumnDataScanState scan_state;
	bool scan_initialized;
};

} // namespace duckdb
