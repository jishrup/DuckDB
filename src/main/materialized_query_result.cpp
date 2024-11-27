#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

MaterializedQueryResult::MaterializedQueryResult(StatementType statement_type, StatementProperties properties,
                                                 vector<string> names_p, unique_ptr<ColumnDataCollection> collection_p,
                                                 ClientProperties client_properties)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type, std::move(properties), collection_p->Types(),
                  std::move(names_p), std::move(client_properties)),
      collection(std::move(collection_p)), scan_initialized(false) {
}

MaterializedQueryResult::MaterializedQueryResult(ErrorData error)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, std::move(error)), scan_initialized(false) {
}

string MaterializedQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[ Rows: " + to_string(collection->Count()) + "]\n";
		auto &coll = Collection();
		for (auto &row : coll.Rows()) {
			for (idx_t col_idx = 0; col_idx < coll.ColumnCount(); col_idx++) {
				if (col_idx > 0) {
					result += "\t";
				}
				auto val = row.GetValue(col_idx);
				result += val.IsNull() ? "NULL" : StringUtil::Replace(val.ToString(), string("\0", 1), "\\0");
			}
			result += "\n";
		}
		result += "\n";
	} else {
		result = GetError() + "\n";
	}
	return result;
}

string MaterializedQueryResult::ToBox(ClientContext &context, const BoxRendererConfig &config) {
	if (!success) {
		return GetError() + "\n";
	}
	if (!collection) {
		return "Internal error - result was successful but there was no collection";
	}
	BoxRenderer renderer(config);
	return renderer.ToString(context, names, Collection());
}

Value MaterializedQueryResult::GetValue(idx_t column, idx_t index) {
	if (!row_collection) {
		row_collection = make_uniq<ColumnDataRowCollection>(collection->GetRows());
	}
	return row_collection->GetValue(column, index);
}

idx_t MaterializedQueryResult::RowCount() const {
	return collection ? collection->Count() : 0;
}

ColumnDataCollection &MaterializedQueryResult::Collection() {
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	if (!collection) {
		throw InternalException("Missing collection from materialized query result");
	}
	return *collection;
}

unique_ptr<ColumnDataCollection> MaterializedQueryResult::TakeCollection() {
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	if (!collection) {
		throw InternalException("Missing collection from materialized query result");
	}
	return std::move(collection);
}

unique_ptr<DataChunk> MaterializedQueryResult::Fetch() {
	return FetchRaw();
}

unique_ptr<DataChunk> MaterializedQueryResult::FetchRaw() {
	if (HasError()) {
		throw InvalidInputException("Attempting to fetch from an unsuccessful query result\nError: %s", GetError());
	}
	auto result = make_uniq<DataChunk>();
	collection->InitializeScanChunk(*result);
	if (!scan_initialized) {
		// we disallow zero copy so the chunk is independently usable even after the result is destroyed
		collection->InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
		scan_initialized = true;
	}
	collection->Scan(scan_state, *result);
	if (result->size() == 0) {
		return nullptr;
	}
	return result;
}

std::vector<std::vector<unique_ptr<Base>>> MaterializedQueryResult::getContents() {
    std::vector<std::vector<unique_ptr<Base>>> overallResult;

    if (!success) {
        return overallResult; // Return an empty result if the query was unsuccessful
    }

    auto &coll = Collection();
    overallResult.reserve(coll.Count()); // Reserve space for the number of rows in advance

    // Iterate over each row in the collection
    for (auto &row : coll.Rows()) {
        std::vector<unique_ptr<Base>> dataVector;
        dataVector.reserve(coll.ColumnCount()); // Reserve space for the number of columns in each row

        // Iterate over each column in the row
        for (idx_t col_idx = 0; col_idx < coll.ColumnCount(); col_idx++) {
            auto val = row.GetValue(col_idx);
            if (!val.IsNull()) {
                // Process based on the column type
                switch (val.type().id()) {
                    case LogicalTypeId::BOOLEAN: {
                        auto native_bool = val.GetValue<bool>();
                        dataVector.push_back(make_uniq<duckdb::BoolData>(native_bool));
                        break;
                    }
                    case LogicalTypeId::BIGINT: {
                        auto native_bigint = val.GetValue<int64_t>();
                        dataVector.push_back(make_uniq<duckdb::BigIntData>(native_bigint));
                        break;
                    }
                    case LogicalTypeId::UBIGINT: {
                        auto native_ubigint = val.GetValue<uint64_t>();
                        dataVector.push_back(make_uniq<duckdb::UBigIntData>(native_ubigint));
                        break;
                    }
                    case LogicalTypeId::INTEGER:
                    case LogicalTypeId::SMALLINT:
                    case LogicalTypeId::TINYINT:
                    case LogicalTypeId::USMALLINT:
                    case LogicalTypeId::UTINYINT: {
                        auto native_int = val.GetValue<int>();
                        dataVector.push_back(make_uniq<duckdb::IntData>(native_int));
                        break;
                    }
                    case LogicalTypeId::UINTEGER: {
                        auto native_uint = val.GetValue<uint32_t>();
                        dataVector.push_back(make_uniq<duckdb::UIntData>(native_uint));
                        break;
                    }
                    case LogicalTypeId::DOUBLE:
                    case LogicalTypeId::FLOAT:
                    case LogicalTypeId::DECIMAL: {
                        auto native_double = val.GetValue<double>();
                        dataVector.push_back(make_uniq<duckdb::DoubleData>(native_double));
                        break;
                    }
                    case LogicalTypeId::VARCHAR: {
                        auto native_string = val.GetValue<std::string>();
                        dataVector.push_back(make_uniq<duckdb::StringData>(native_string));
                        break;
                    }
                    default:
                        // Handle unsupported or custom types as strings
                        auto native_string = val.ToString();
                        dataVector.push_back(make_uniq<duckdb::StringData>(native_string));
                        break;
                }
            } else {
                // Handle nulls if necessary (e.g., skip or add a placeholder)
                dataVector.push_back(nullptr);
            }
        }
        overallResult.push_back(std::move(dataVector));
    }

    return overallResult;
}


} // namespace duckdb
