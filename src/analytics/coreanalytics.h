// Copyright (c) 2017-2022 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_ANALYTICS_COREANALYTICS_H
#define BITCOIN_ANALYTICS_COREANALYTICS_H

#include <analytics/baseanalytic.h>
#include <fstream>
#include <chrono>
#include <arrow/table.h>

static constexpr bool DEFAULT_COREANALYTICS{false};

/**
 * TxIndex is used to look up transactions included in the blockchain by hash.
 * The index is written to a LevelDB database and records the filesystem
 * location of each transaction by transaction hash.
 */
struct CoreAnalyticsRow {
    int64_t height;
    double issuance;
    double transaction_fees;
    double miner_revenue;
    double puell_multiple;
    double bdd;
    double asopr;
    double rc;
    double rp;
    double rl;
    double nrpl;
    double rplr;
    double cs;
    double mc;
    double mvrv;
    double mvrv_z;
    double realized_price;
    double rpv_ratio;
    double utxos_in_loss;
    double utxos_in_profit;
    double percent_utxos_in_profit;
    double percent_supply_in_profit;
    double total_supply_in_loss;
    double total_supply_in_profit;
    double ul;
    double up;
    double rul;
    double rup;
    double abdd;
    double vocd;
    double mvocd;
    double hodl_bank;
    double reserve_risk;
    double ath;
    double dfath;
};
struct TempVars {
    int64_t inputs;
};

struct UtxoMapEntry {
    uint64_t timestamp;
    double price;
    int64_t utxo_count;
    double utxo_amount;
};

class CoreAnalytics final : public BaseAnalytic
{
protected:
    class DB;

private:
    const std::unique_ptr<DB> m_db;
    std::unordered_map<int64_t, UtxoMapEntry> m_utxo_map;
    CoreAnalyticsRow m_row;
    TempVars m_temp_vars;
    std::ofstream log_stream;
    std::ofstream perf_stream;

    std::chrono::steady_clock::time_point point1;
    std::chrono::steady_clock::time_point point2;
    std::chrono::steady_clock::time_point point3;
    std::chrono::steady_clock::time_point point4;

    bool AllowPrune() const override { return false; }
    std::unordered_map<int64_t, double> LoadBTCPrices(const std::string& file_path);
    bool ProcessTransactions(const interfaces::BlockInfo& block, const CBlockUndo& blockUndo);
    bool UpdatePriceMap(const interfaces::BlockInfo& block);
    bool CalculateUtxoMetrics();




protected:
    bool CustomAppend(const interfaces::BlockInfo& block) override;
    bool CustomCommit() override; //Needs to update utxo distribution map
    BaseAnalytic::DB& GetDB() const override;

public:
    explicit CoreAnalytics(std::unique_ptr<interfaces::Chain> chain, const fs::path& path);

    // Destructor is declared because this class contains a unique_ptr to an incomplete type.
    virtual ~CoreAnalytics() override;

};

/// The global coreanalytics. May be null.
extern std::unique_ptr<CoreAnalytics> g_coreanalytics;

#endif // 
