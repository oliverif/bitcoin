#include "ohlcvp.h"
#include "ohlcvp.h"
#include "ohlcvp.h"
#include <analytics/ohlcvp.h>

#include <arrow/array.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <clientversion.h>
#include <common/args.h>
#include <core_io.h>
#include <index/txtimestampindex.h>
#include <logging.h>
#include <node/blockstorage.h>
#include <undo.h>
#include <univalue.h>
#include <validation.h>


std::unique_ptr<Ohlcvp> g_ohlcvp;


/** Access to the analytics database (analytics/) */
class Ohlcvp::DB : public BaseAnalytic::DB
{
public:
    explicit DB(const fs::path& path, std::string column_name);

    /// Read the disk location of the transaction data with the given hash. Returns false if the
    /// transaction hash is not indexed.
    // bool WriteOhlcvp(const std::pair<uint64_t,std::vector<double>>& vOhlcvp) const;

    /// Write a batch of transaction positions to the DB.
    [[nodiscard]] bool WriteOhlcvp(const AnalyticsRow& vOhlcvp);
};

Ohlcvp::DB::DB(const fs::path& path, std::string column_name) : BaseAnalytic::DB(
    StorageUtils::AnalyticStorageConfig{
        .analytic_id = "ohlcvp",
        .db_path = gArgs.GetDataDirNet() / "analytics" / "analytics.db ",
        .sqlite_db = nullptr,
        .table_name = "analytics",
        .columns = {
            {"height", "PRIMARY INTEGER"},
            {"original_timestamp", "INTEGER"},
            {"timestamp", "INTEGER"},
            {"open", "REAL"},
            {"high", "REAL"},
            {"low", "REAL"},
            {"close", "REAL"},
            {"volume", "REAL"},
            {"price", "REAL"}
        }})
{
}


bool Ohlcvp::DB::WriteOhlcvp(const AnalyticsRow& vOhlcvp)
{
    return WriteAnalytics(vOhlcvp);
}

Ohlcvp::Ohlcvp(std::unique_ptr<interfaces::Chain> chain, const fs::path& path)
    : BaseAnalytic(std::move(chain), "ohlcvp"), m_db(std::make_unique<Ohlcvp::DB>(path, "ohlcvp"))
{
    std::string csv_file_path = "C:\\Users\\Oliver\\Code\\CryptoTrader\\data\\block_prices_usd.csv";


    log_stream = std::ofstream("D:/Code/bitcoin/error_log.txt", std::ios::app);
    perf_stream = std::ofstream("D:/Code/bitcoin/performance_log.txt", std::ios::app);
}

Ohlcvp::~Ohlcvp()
{
    perf_stream.close();
    log_stream.close();
    BaseAnalytic::~BaseAnalytic();
}

bool Ohlcvp::LoadCsvToBatch(const std::string& file_path, AnalyticsBatch& out_batch)
{
    std::ifstream file(file_path);
    if (!file.is_open()) {
        LogError("Could not open CSV file: " + file_path);
        return false;
    }

    std::string line;
    size_t line_num = 0;

    while (std::getline(file, line)) {
        ++line_num;
        std::stringstream ss(line);
        std::string cell;
        std::vector<std::string> cells;

        // Split line by comma
        while (std::getline(ss, cell, ',')) {
            cells.push_back(cell);
        }

        if (cells.empty()) {
            continue; // Skip empty lines
        }

        // Parse first column as uint64_t key
        AnalyticsRow values;
        try {
            values.first = std::stoull(cells[0]);
        } catch (const std::exception& e) {
            LogError("Invalid key at line " + std::to_string(line_num) + ": " + e.what());
            return false;
        }

        for (size_t i = 1; i < cells.size(); ++i) {
            const std::string& str = cells[i];

            try {
                if (str.find('.') != std::string::npos || str.find('e') != std::string::npos || str.find('E') != std::string::npos) {
                    values.second.emplace_back(std::stod(str));
                } else {
                    values.second.emplace_back(std::stoll(str));
                }
            } catch (const std::exception& e) {
                LogError("Invalid numeric value at line " + std::to_string(line_num) +
                         ", column " + std::to_string(i + 1) + ": " + e.what());
                return false;
            }
        }

        out_batch.emplace_back(values);
    }

    return true;
}

bool Ohlcvp::GetKlines(const interfaces::BlockInfo& block)
{
    int64_t start_height = 

    // 1. Step: Get last synced timestamp from SQLite DB
    int64_t start_timestamp;
    {
        auto stmt = m_db->Prepare("SELECT MAX(timestamp) FROM analytics;");
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            start_timestamp = sqlite3_column_int64(stmt, 0);
        } else {
            log_stream << "Failed to read last timestamp from DB\n";
            return false;
        }
        sqlite3_finalize(stmt);
    }

    // 2. Step: Get all blocks from last timestamp up to tip
    std::vector<std::pair<int64_t, uint64_t>> raw_blocks; // timestamp, height
    const CBlockIndex* pindex = WITH_LOCK(cs_main, return m_chainstate->m_blockman.LookupBlockIndex(m_chain->getTipHash()));
    while (pindex && pindex->GetBlockTime() > start_timestamp) {
        raw_blocks.emplace_back(pindex->GetBlockTime(), pindex->nHeight);
        pindex = pindex->pprev;
    }
    std::reverse(raw_blocks.begin(), raw_blocks.end());

    if (raw_blocks.empty()) return true; // Already synced

    // 3. Step: Clean block times to be strictly increasing
    for (size_t i = 1; i < raw_blocks.size(); ++i) {
        if (raw_blocks[i].first <= raw_blocks[i - 1].first) {
            raw_blocks[i].first = raw_blocks[i - 1].first + 1;
        }
    }

    // 4. Step: Prepare kline API request
    int64_t end_timestamp = raw_blocks.back().first;
    int64_t request_start = start_timestamp * 1000; // ms
    int64_t request_end = end_timestamp * 1000;

    int64_t interval_ms = 60 * 1000;
    int64_t max_duration = 1000 * interval_ms;
    if (request_end - request_start > max_duration) {
        request_end = request_start + max_duration;
    }

    std::string url = "https://api.binance.com/api/v3/klines?symbol=" + symbol +
                      "&interval=1m&startTime=" + std::to_string(request_start) +
                      "&endTime=" + std::to_string(request_end);

    std::string response_json;
    if (!HttpGet(url, response_json)) {
        log_stream << "Failed to fetch klines from Binance\n";
        return false;
    }

    UniValue json;
    if (!json.read(response_json)) {
        log_stream << "Failed to parse Binance kline JSON\n";
        return false;
    }

    // 5. Step: Parse response and bucket into blocks
    size_t kline_idx = 0;
    std::vector<AnalyticsRow> analytics_rows;
    for (size_t i = 1; i < raw_blocks.size(); ++i) {
        int64_t start_time = raw_blocks[i - 1].first * 1000;
        int64_t end_time = raw_blocks[i].first * 1000;

        double open = -1, high = -1, low = -1, close = -1, volume = 0, price = 0;
        int count = 0;

        while (kline_idx < json.size()) {
            int64_t kline_time = json[kline_idx][0].get_int64();
            if (kline_time >= end_time) break;
            if (kline_time >= start_time) {
                double o = std::stod(json[kline_idx][1].get_str());
                double h = std::stod(json[kline_idx][2].get_str());
                double l = std::stod(json[kline_idx][3].get_str());
                double c = std::stod(json[kline_idx][4].get_str());
                double v = std::stod(json[kline_idx][5].get_str());
                if (open < 0) open = o;
                high = (high < 0) ? h : std::max(high, h);
                low = (low < 0) ? l : std::min(low, l);
                close = c;
                volume += v;
                price += c;
                count++;
            }
            ++kline_idx;
        }

        if (count > 0) {
            AnalyticsRow row;
            row.first = raw_blocks[i].second; // height
            row.second = {
                raw_blocks[i - 1].first, // original block time
                raw_blocks[i].first,     // cleaned timestamp
                open, high, low, close, volume,
                price / count // avg price
            };
            analytics_rows.push_back(row);
        }
    }

    // 6. Step: Write to DB
    if (!analytics_rows.empty() && !m_db->WriteOhlcvp(analytics_rows)) {
        log_stream << "Failed to write klines to DB\n";
        return false;
    }

    return true;

    return false;
}

bool Ohlcvp::CustomAppend(const interfaces::BlockInfo& block)
{
    // Exclude genesis block transaction because outputs are not spendable.
    if (block.height == 0) return true;

    assert(block.data);

    uint64_t blockTime = block.data->GetBlockHeader().nTime;

    CBlockUndo block_undo;
    const CBlockIndex* pindex = WITH_LOCK(cs_main, return m_chainstate->m_blockman.LookupBlockIndex(block.hash));
    if (!m_chainstate->m_blockman.ReadBlockUndo(block_undo, *pindex)) {
        return false;
    }


    return false;
}

bool Ohlcvp::CustomInit(const std::optional<interfaces::BlockRef>& block)
{
    if (!block) {
        AnalyticsBatch out_batch;
        if (!LoadCsvToBatch("D:\\Code\bitcoin\\ohlcvp.csv", out_batch)) {
            LogError("Could not load backfill csv for ohlcvp");
            return false;
        }
        if (!GetDB().WriteAnalytics(out_batch)) {
            LogError("Could not write backfill data to db for ohlcvp");
            return false;
        }
        auto last_height = out_batch.back().first;
        const CBlockIndex* locator_index{m_chainstate->m_blockman.LookupBlockIndex(m_chain->getBlockHash(last_height))};
        SetBestBlockIndex(locator_index);
        
    }
    return true;
}

BaseAnalytic::DB& Ohlcvp::GetDB() const { return *m_db; }
