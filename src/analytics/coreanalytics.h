// Copyright (c) 2017-2022 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_ANALYTICS_COREANALYTICS_H
#define BITCOIN_ANALYTICS_COREANALYTICS_H

#include <analytics/baseanalytic.h>
#include <analytics/runningmedian.h>
#include <fstream>
#include <chrono>
#include <arrow/table.h>
#include <index/coinstatsindex.h>
#include <kernel/coinstats.h>

static constexpr bool DEFAULT_COREANALYTICS{false};
/**
 * TxIndex is used to look up transactions included in the blockchain by hash.
 * The index is written to a LevelDB database and records the filesystem
 * location of each transaction by transaction hash.
 */
struct CoreAnalyticsRow {
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
    int64_t inputs = 0;
    double spendable_out = 0;
    int64_t delta_ntransaction_outputs = 0;
    double coinbase_amount = 0;
    double previous_utxo_value = 0;

    CAmount previous_total_new_out;
    CAmount previous_total_coinbase_amount;
    uint64_t previous_nTransactionOutputs;
    RunningStats mvrv_stats;
    RunningStats miner_rev_stats;
    RunningMedian mvocd_running;
    double previous_ath;
    double preveious_hodl_bank;
};

struct UtxoMapEntry {
    uint64_t timestamp;
    double price;
    int64_t utxo_count;
    double utxo_amount;

    void Serialize(DataStream& ds) const
    {
        ds << timestamp << price << utxo_count << utxo_amount;
    }
    void Deserialize(DataStream& ds)
    {
        ds >> timestamp >> price >> utxo_count >> utxo_amount;
    }
};
using UtxoMap = std::unordered_map<int64_t, UtxoMapEntry>;

struct RunningStats {
    int n = 0;
    double mean = 0.0;
    double M2 = 0.0; // Sum of squared diffs from the mean

    void init_from_data(const std::vector<double>& data);
    void add(double x);
    void remove(const std::vector<double>& xs);
    double variance() const;
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
    double m_current_price;
    uint64_t m_current_timestamp;
    uint64_t m_current_height;
    std::ofstream log_stream;
    std::ofstream perf_stream;

    std::chrono::steady_clock::time_point point1;
    std::chrono::steady_clock::time_point point2;
    std::chrono::steady_clock::time_point point3;
    std::chrono::steady_clock::time_point point4;

    bool AllowPrune() const override { return false; }
    std::unordered_map<int64_t, double> LoadBTCPrices(const std::string& file_path);
    bool UpdatePriceMap();
    bool ProcessTransactions(const interfaces::BlockInfo& block, const CBlockUndo& blockUndo);
    bool PrepareStatistics(const interfaces::BlockInfo& block);
    bool GetIndexData(const interfaces::BlockInfo& block, const CBlockIndex& block_index);
    bool CalculateUtxoMetrics(const interfaces::BlockInfo& block);
    bool UpdateMeanVars(const interfaces::BlockInfo& block);
    uint64_t GetHeightAfterTimestamp(uint64_t timestamp, uint64_t height);
    bool UpdateVocdMedian();
    void SerializeUtxoMap(const UtxoMap& map, DataStream& s);
    void DeserializeUtxoMap(UtxoMap& map, DataStream& s);



protected:
    bool CustomAppend(const interfaces::BlockInfo& block) override;
    bool CustomCommit() override; //Needs to update utxo distribution map
    bool CustomRewind(const interfaces::BlockRef& current_tip, const interfaces::BlockRef& new_tip) override; //TODO: implement rewind logic
    BaseAnalytic::DB& GetDB() const override;
    bool CustomInit(const std::optional<interfaces::BlockRef>& block) override;

    bool LoadUtxoMap();

public:
    explicit CoreAnalytics(std::unique_ptr<interfaces::Chain> chain, const fs::path& path);

    // Destructor is declared because this class contains a unique_ptr to an incomplete type.
    virtual ~CoreAnalytics() override;

};

/// The global coreanalytics. May be null.
extern std::unique_ptr<CoreAnalytics> g_coreanalytics;

#endif // 
