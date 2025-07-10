// Copyright (c) 2017-2022 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_ANALYTICS_OHLCVP_H
#define BITCOIN_ANALYTICS_OHLCVP_H

#include <analytics/baseanalytic.h>
#include <fstream>
#include <chrono>
#include <arrow/table.h>

static constexpr bool DEFAULT_OHLCVP{false};

/**
 * TxIndex is used to look up transactions included in the blockchain by hash.
 * The index is written to a LevelDB database and records the filesystem
 * location of each transaction by transaction hash.
 */
class Ohlcvp final : public BaseAnalytic
{
protected:
    class DB;

private:
    const std::unique_ptr<DB> m_db;
    std::ofstream log_stream;
    std::ofstream perf_stream;

    std::chrono::steady_clock::time_point point1;
    std::chrono::steady_clock::time_point point2;
    std::chrono::steady_clock::time_point point3;
    std::chrono::steady_clock::time_point point4;

    bool AllowPrune() const override { return false; }
    bool LoadCsvToBatch(const std::string& file_path, AnalyticsBatch& out_batch);
    bool GetKlines(const interfaces::BlockInfo& block);

protected:
    bool CustomAppend(const interfaces::BlockInfo& block) override;
    bool CustomInit(const std::optional<interfaces::BlockRef>& block) override;

    BaseAnalytic::DB& GetDB() const override;

public:
    explicit Ohlcvp(std::unique_ptr<interfaces::Chain> chain, const fs::path& path);

    // Destructor is declared because this class contains a unique_ptr to an incomplete type.
    virtual ~Ohlcvp() override;

};

/// The global ohlcvp. May be null.
extern std::unique_ptr<Ohlcvp> g_ohlcvp;

#endif // BITCOIN_ANALYTICS_Ohlcvp_H
