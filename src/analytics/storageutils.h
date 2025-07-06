#pragma once

#include <string>
#include <vector>
#include <sqlite3.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace StorageUtils {


struct AnalyticStorageConfig {
    std::string analytic_id;
    std::string db_name;
    sqlite3* sqlite_db;
    std::string table_name;
    std::vector<std::string> columns;
};

inline bool ValidateConfig(AnalyticStorageConfig& config, bool create_if_missing) {

    if (!TableExists(config, create_if_missing)) {
        return false;
    }
    if (!ColumnsExist(config, create_if_missing)) {
        return false;
    }
}
inline bool dbExist(AnalyticStorageConfig& config, bool create_if_missing) {

    if (!fs::exists()) {
        
    }
}

inline bool TableExists(AnalyticStorageConfig& config, bool create_if_missing)
{
    const char* check_table_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(config.sqlite_db, check_table_sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, config.table_name.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            sqlite3_finalize(stmt);
            return true;
        } else if (create_if_missing) {
            return CreateTable(config);
        }
        sqlite3_finalize(stmt);
        return false;
    }
    LogError("Failed to prepare table check: %s", sqlite3_errmsg(m_db));

    return false;
}

inline bool ColumnsExist(AnalyticStorageConfig& config, bool create_if_missing)
{

    std::string query = "PRAGMA table_info(" + config.table_name + ");";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(m_db, query.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(m_db) << std::endl;
        return false;
    }

    std::vector<std::string> found_columns;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1)); // column 1 is "name"
        if (name) {
            found_columns.insert(name);
        }
    }
    sqlite3_finalize(stmt);

    // 3. Verify all expected columns are found
    std::vector<std::string> missing;
    for (const auto& col : config.columns) {
        if (!found_columns.contains(col)) {
            missing.push_back(col);
        }
    }
    if (!missing.empty()) {
        if (create_if_missing) {
            return AddColumns(config);
        } else {
            LogError("Column %s does not exist in table %s\n", col, config.table_name);
            return false;
        }
    }
    return true;
}

bool CreateTable(AnalyticStorageConfig& config)
{
    // Combined SQL string to create both tables
    std::string sql = "CREATE TABLE IF NOT EXISTS " + config.table_name + " (\n";
    sql += "    height INTEGER PRIMARY KEY";

    for (const auto& column : config.column_names) {
        sql += ",\n    " + column + " REAL";
    }

    sql += "\n);";

    sql += R"(
CREATE TABLE IF NOT EXISTS sync_points (
    analytic_id TEXT PRIMARY KEY,
    locator BLOB
);)";

    char* err_msg = nullptr;
    if (sqlite3_exec(m_db, sql.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK) {
        std::cerr << "SQL error (creating tables): " << err_msg << std::endl;
        sqlite3_free(err_msg);
    }
    return true;
}

bool AddColumns(AnalyticStorageConfig& config)
{
    bool success = true;

    for (const auto& column_name : config.columns) {
        std::string alter_sql = "ALTER TABLE " + config.table_name + " ADD COLUMN " + column_name + " REAL;";
        char* err_msg = nullptr;

        if (sqlite3_exec(m_db, alter_sql.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK) {
            std::cerr << "SQL error when adding column '" << column_name << "': " << err_msg << std::endl;
            sqlite3_free(err_msg);
            success = false;
        }
    }

    return success;
}

} // namespace StorageUtils

