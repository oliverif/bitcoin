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
class CoreAnalytics final : public BaseAnalytic
{
protected:
    class DB;

private:
    const std::unique_ptr<DB> m_db;
    std::unordered_map<int64_t, std::pair<int64_t, double>> btc_price_map;
    std::unordered_map<int64_t, std::pair<int64_t, double>> utxo_distribution_map;
    std::ofstream log_stream;
    std::ofstream perf_stream;

    std::chrono::steady_clock::time_point point1;
    std::chrono::steady_clock::time_point point2;
    std::chrono::steady_clock::time_point point3;
    std::chrono::steady_clock::time_point point4;

    bool AllowPrune() const override { return false; }
    std::unordered_map<int64_t, double> LoadBTCPrices(const std::string& file_path);
    bool ProcessTransactions(const CBlock& block, const CBlockUndo& blockUndo);
    bool UpdatePriceMap(const interfaces::BlockInfo& block);



protected:
    bool CustomAppend(const interfaces::BlockInfo& block) override;

    BaseAnalytic::DB& GetDB() const override;

public:
    explicit CoreAnalytics(std::unique_ptr<interfaces::Chain> chain, const fs::path& path);

    // Destructor is declared because this class contains a unique_ptr to an incomplete type.
    virtual ~CoreAnalytics() override;

};

/// The global coreanalytics. May be null.
extern std::unique_ptr<CoreAnalytics> g_coreanalytics;

#endif // 
