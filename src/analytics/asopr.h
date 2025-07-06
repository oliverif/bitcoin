// Copyright (c) 2017-2022 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_ANALYTICS_ASOPR_H
#define BITCOIN_ANALYTICS_ASOPR_H

#include <analytics/baseanalytic.h>
#include <fstream>
#include <chrono>
#include <arrow/table.h>

static constexpr bool DEFAULT_ASOPR{false};

/**
 * TxIndex is used to look up transactions included in the blockchain by hash.
 * The index is written to a LevelDB database and records the filesystem
 * location of each transaction by transaction hash.
 */
class Asopr final : public BaseAnalytic
{
protected:
    class DB;

private:
    const std::unique_ptr<DB> m_db;
    std::unordered_map<int64_t, double> btc_price_map;
    std::ofstream log_stream;
    std::ofstream perf_stream;

    std::chrono::steady_clock::time_point point1;
    std::chrono::steady_clock::time_point point2;
    std::chrono::steady_clock::time_point point3;
    std::chrono::steady_clock::time_point point4;

    bool AllowPrune() const override { return false; }
    std::unordered_map<int64_t, double> LoadBTCPrices(const std::string& file_path);
    std::optional<double> CalculateASOPR(const CBlock& block, const CBlockUndo& blockUndo, const std::unordered_map<int64_t, double>& btc_price_map);
    std::optional<double> GetBTCPrice(const std::unordered_map<int64_t, double>& btc_price_map, int64_t timestamp, std::ofstream& log_stream);
    std::unordered_map<int64_t, double> ConvertTableToMap(const std::shared_ptr<arrow::Table>& table);

protected:
    bool CustomAppend(const interfaces::BlockInfo& block) override;

    BaseAnalytic::DB& GetDB() const override;

public:
    explicit Asopr(std::unique_ptr<interfaces::Chain> chain, const fs::path& path);

    // Destructor is declared because this class contains a unique_ptr to an incomplete type.
    virtual ~Asopr() override;

};

/// The global asopr. May be null.
extern std::unique_ptr<Asopr> g_asopr;

#endif // BITCOIN_ANALYTICS_Asopr_H
