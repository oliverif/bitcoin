// Copyright (c) 2017-present The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <chainparams.h>
#include <common/args.h>
#include <analytics/analyticsmanager.h>
#include <interfaces/chain.h>
#include <kernel/chain.h>
#include <logging.h>
#include <node/abort.h>
#include <node/blockstorage.h>
#include <node/context.h>
#include <node/database_args.h>
#include <node/interface_ui.h>
#include <tinyformat.h>
#include <util/string.h>
#include <util/thread.h>
#include <util/translation.h>
#include <validation.h> // For g_chainman

#include <string>
#include <utility>


constexpr auto SYNC_LOG_INTERVAL{30s};
constexpr auto SYNC_LOCATOR_WRITE_INTERVAL{30s};

template <typename... Args>



AnalyticsManager::AnalyticsManager(std::vector<BaseAnalytic*>& analytics) : activeAnalytics(analytics)
{

}

bool AnalyticsManager::Init()
{
    if (!InitDataStorage()) {
        LogError("Could not initialize data storage\n");
        return false;
    }
        

}

bool AnalyticsManager::InitDataStorage()
{
    std::unordered_map < std::string, std::unordered_map < std::string, std::unordered_set < std::string >>>> table_to_columns;

    for (const auto& analytic : analytics) {
        const auto& config = analytic->GetDB()->getStorageConfig();
        auto& column_set = table_to_columns[config.table_name];

        for (const auto& col : config.columns) {
            if (!std::find(column_set.begin(), column_set.end(), col) != column_set.end()) {
                LogError("Duplicate column definitions for %s\n", col);
                return false;
            }
            column_set.insert(col);
        }
    }

    for (const auto& [table, columns] : table_to_columns) {

    }

}

bool AnalyticsManager::InitAnalytics()
{
    for (auto analytic : activeAnalytics)
        if (!analytic->Init()) return false;

    return true;
}

bool AnalyticsManager::ValidateDbConfig()
{
    // Check that db path exists
    if (!std::filesystem::exists(m_db_path.parent_path())) {
        throw std::runtime_error("Db path doesn't exist:" + m_db_path.parent_path().string());
        // std::filesystem::create_directories(m_db_path.parent_path()); // Create the directory if it doesn't exist
    }
    if (!std::filesystem::exists(m_db_path)) {
        throw std::runtime_error("Db file doesn't exist:" + m_db_path.generic_string());
        // std::filesystem::create_directories(m_db_path.parent_path()); // Create the directory if it doesn't exist
    }

    // Check that table and columns exist
    auto config = getStorageConfig();
    if (sqlite3_open_v2(m_db_path.utf8string().c_str(), &m_db, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
        std::string error_msg = "Can't open database: " + std::string(sqlite3_errmsg(m_db));
        sqlite3_close(m_db); // Ensure the database is closed if open failed
        throw std::runtime_error(error_msg);
    }

    if (!TableExists()) {
        return false;
    }
    if (!ColumnsExist()) {
        return false;
    }
    return true;
}
bool AnalyticsManager::TableExists()
{
    auto config = getStorageConfig();
    const char* check_table_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(m_db, check_table_sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, config.table_name.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            sqlite3_finalize(stmt);
            return true;
        }
        sqlite3_finalize(stmt);
        LogError("Table %s does not exist\n", config.table_name);
        return false;
    }
    LogError("Failed to prepare table check: %s", sqlite3_errmsg(m_db));

    return false;
}


bool AnalyticsManager::ColumnsExist()
{
    auto config = getStorageConfig();

    std::string query = "PRAGMA table_info(" + config.table_name + ");";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(m_db, query.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(m_db) << std::endl;
        return false;
    }

    std::set<std::string> found_columns;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1)); // column 1 is "name"
        if (name) {
            found_columns.insert(name);
        }
    }
    sqlite3_finalize(stmt);

    // 3. Verify all expected columns are found
    for (const auto& col : config.columns) {
        if (found_columns.find(col) == found_columns.end()) {
            LogError("Column %s does not exist in table %s\n", col, config.table_name);
            return false; // at least one expected column not found
        }
    }
    return true; // Column does not exist
}
