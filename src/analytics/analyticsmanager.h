// Copyright (c) 2017-present The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_ANALYTICS_MANAGER_H
#define BITCOIN_ANALYTICS_MANAGER_H

#include <dbwrapper.h>
#include <interfaces/chain.h>
#include <interfaces/types.h>
#include <util/string.h>
#include <util/threadinterrupt.h>
#include <validationinterface.h>
#include <sqlite3.h>
#include <analytics/baseanalytic.h>

#include <string>

class CBlock;
class CBlockIndex;
class Chainstate;
class ChainstateManager;
class BaseAnalytic;
namespace interfaces {
class Chain;
} // namespace interfaces



class AnalyticsManager
{

private:
  
    const uint64_t start_timestamp{1382330735};
    sqlite3* m_db;
    const fs::path& m_db_path;
    std::string column_name;
    std::vector<BaseAnalytic*>& activeAnalytics;

    bool ColumnExists(const std::string& column_name);
    void CreateAnalyticsTable(const std::string& column_name);
    void AddColumn(const std::string& column_name);
    bool InitAnalytics();
    bool InitDataStorage();
    bool ColumnsExist();
    bool TableExists();
    bool ValidateDbConfig();

protected:
    std::unique_ptr<interfaces::Chain> m_chain;
    Chainstate* m_chainstate{nullptr};
    const std::string m_name;




public:
    AnalyticsManager(std::vector<BaseAnalytic*>& analytics);
    /// Destructor interrupts sync thread if running and blocks until it exits.
    ~AnalyticsManager();

    /// Get the name of the analytics for display in logs.
    const std::string& GetName() const LIFETIMEBOUND { return m_name; }


    
    bool Init();

};

#endif // BITCOIN_ANALYTICS_MANAGER_H
