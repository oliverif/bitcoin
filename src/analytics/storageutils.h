#ifndef STORAGE_UTILS_H
#define STORAGE_UTILS_H

#include <string>
#include <vector>
#include <sqlite3.h>
#include <util/fs.h>
#include <variant>
#include <logging.h>


namespace StorageUtils {


struct ColumnSpec {
    std::string name;
    std::string sqlite_type;
};

struct AnalyticStorageConfig {
    std::string analytic_id;
    fs::path db_path;
    sqlite3* sqlite_db;
    std::string table_name;
    std::vector<ColumnSpec> columns;
};

inline bool OpenDb(AnalyticStorageConfig& config, bool create_if_missing)
{
    if (sqlite3_open_v2(config.db_path.utf8string().c_str(), &config.sqlite_db, create_if_missing ? SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE : SQLITE_OPEN_READWRITE, nullptr) != SQLITE_OK) {
        std::string error_msg = "Can't open database: " + std::string(sqlite3_errmsg(config.sqlite_db));
        sqlite3_close(config.sqlite_db); // Ensure the database is closed if open failed
        LogError("%s: %s\n",__func__,error_msg);
        return false;
    }
    return true;
}

inline bool dbExist(AnalyticStorageConfig& config, bool create_if_missing)
{
    if (!fs::exists(config.db_path.parent_path())) {
        LogError("%s: Path to db does not exist: %s\n",__func__,config.db_path.parent_path().string());
        return false;
    } else {
        return OpenDb(config, create_if_missing);
    }
}

inline bool CreateTable(AnalyticStorageConfig& config)
{
    // Combined SQL string to create both tables
    std::string sql = "CREATE TABLE IF NOT EXISTS " + config.table_name + " (\n";
    bool first = true;
    for (const auto& column : config.columns) {
        if (!first) sql += ",\n";
        sql += "    " + column.name + " " + column.sqlite_type;
        first = false;
    }

    sql += "\n);";

    sql += R"(CREATE TABLE IF NOT EXISTS sync_points (
            analytic_id TEXT PRIMARY KEY,
            locator BLOB
        );)";

    char* err_msg = nullptr;
    if (sqlite3_exec(config.sqlite_db, sql.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK) {
        LogError("%s: SQL error (creating tables): %s",__func__,std::string(err_msg));
        sqlite3_free(err_msg);
    }
    return true;
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
    LogError("%s: Failed to prepare table check: %s",__func__, std::string(sqlite3_errmsg(config.sqlite_db)));

    return false;
}

inline bool AddColumns(AnalyticStorageConfig& config, std::vector<ColumnSpec> columns)
{
    bool success = true;

    for (const auto& column : columns) {
        std::string alter_sql = "ALTER TABLE " + config.table_name + " ADD COLUMN " + column.name + " " + column.sqlite_type + ";";
        char* err_msg = nullptr;

        if (sqlite3_exec(config.sqlite_db, alter_sql.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK) {
            LogError("%s: SQL error when adding column '%s': %s",__func__,column.name,err_msg);
            sqlite3_free(err_msg);
            success = false;
        }
    }

    return success;
}

inline bool ColumnsExist(AnalyticStorageConfig& config, bool create_if_missing)
{
    std::string query = "PRAGMA table_info(" + config.table_name + ");";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(config.sqlite_db, query.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        LogError("%s: Failed to prepare statement: %s",__func__,std::string(sqlite3_errmsg(config.sqlite_db)));
        return false;
    }

    std::vector<std::string> found_columns;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1)); // column 1 is "name"
        if (name) {
            found_columns.emplace_back(std::string(name));
        }
    }
    sqlite3_finalize(stmt);

    // 3. Verify all expected columns are found
    std::vector<ColumnSpec> missing;
    for (const auto& col : config.columns) {
        if (!(std::find(found_columns.begin(), found_columns.end(), col.name) != found_columns.end())) {
            missing.push_back(col);
        }
    }
    if (!missing.empty()) {
        if (create_if_missing) {
            return AddColumns(config, missing);
        } else {
            LogError("%s: Columns for analytic '%s' does not exist in table %s",__func__, config.analytic_id,config.table_name);
            return false;
        }
    }
    return true;
}

inline bool ValidateConfig(AnalyticStorageConfig& config, bool create_if_missing) {
    if (!dbExist(config, create_if_missing)) {
        return false;
    }
    if (!TableExists(config, create_if_missing)) {
        return false;
    }
    if (!ColumnsExist(config, create_if_missing)) {
        return false;
    }
    return true;
}

inline bool BindValue(sqlite3_stmt* stmt, int index, const std::string& type, const std::variant<int64_t, double>& value)
{
    if (type == "INTEGER") {
        return sqlite3_bind_int64(stmt, index, static_cast<int64_t>(std::get<double>(value))) == SQLITE_OK;
    } else if (type == "REAL") {
        return sqlite3_bind_double(stmt, index, std::get<double>(value)) == SQLITE_OK;
    } else {
        LogError("%s: Unsupported SQLite column type: %s",__func__,type);
        return false;
    }
}










} // namespace StorageUtils

#endif
