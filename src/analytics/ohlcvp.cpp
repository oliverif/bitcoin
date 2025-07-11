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
#include <curl/curl.h>
#include <sstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
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
            {"height", "INTEGER PRIMARY KEY"},
            {"original_blocktime", "INTEGER"},
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
        LogError("%s: Could not open CSV file: %s\n", __func__, file_path);
        return false;
    }
    std::string line;
    bool first_line = true;

    while (std::getline(file, line)) {
        if (first_line) {
            first_line = false; // Skip header row
            continue;
        }
        if (line.empty()) continue;

        std::stringstream ss(line);
        std::string cell;

        // Read height (first column)
        if (!std::getline(ss, cell, ',')) {
            LogError("%s: Malformed line (missing height):%s",__func__,line);
            return false;
        }
        AnalyticsRow values;
        values.first = 0;
        try {
            values.first = std::stoull(cell);
        } catch (const std::exception& e) {
            LogError("%s: Invalid height value '%s' in line: %s",__func__,cell, line);
            return false;
        }


        while (std::getline(ss, cell, ',')) {
            try {
                double val = std::stod(cell);
                values.second.push_back(val);
            } catch (const std::exception& e) {
                LogError("%s: Invalid double value '%s' in line: %s",__func__,cell,line);
                return false;
            }
        }

        out_batch.emplace_back(values);
    }

    return true;
}



bool Ohlcvp::HttpGet(const std::string& url, json& response)
{
    CURL* curl = curl_easy_init();
    if (!curl) {
        LogError("%s: Failed to initialize CURL\n", __func__);
        return false;
    }

    std::ostringstream response_stream;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L); // Follow redirects
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);       // Timeout after 10 seconds

    // Write callback
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, +[](char* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
            auto* stream = static_cast<std::ostringstream*>(userdata);
            stream->write(ptr, size * nmemb);
            return size * nmemb; });
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_stream);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        LogError("%s: CURL request failed: %s\n", __func__, curl_easy_strerror(res));
        curl_easy_cleanup(curl);
        return false;
    }

    curl_easy_cleanup(curl);
    std::string response_body = response_stream.str();
    try {
        response = json::parse(response_body);
    } catch (const std::exception& e) {
        LogError("%s: Failed to parse JSON: %s\n", __func__, e.what());
        return false;
    }
    return true;
}

bool Ohlcvp::GetKlines(const interfaces::BlockInfo& block, AnalyticsRow& new_row)
{
    int64_t start_height = block.height - 1;
    AnalyticsRow prev_row;
    if (!GetDB().ReadAnalytics(prev_row, GetDB().GetStorageConfig().columns, start_height)) {
        LogError("%s: Could not read previous row\n", __func__);
        return false;
    }
    int64_t start_timestamp = std::get<int64_t>(prev_row.second[2]);

    int64_t end_height = block.height;
    int64_t end_timestamp = block.chain_time_max;
    new_row = prev_row;
    new_row.second[1] = block.data->GetBlockTime();
    new_row.second[2] = block.chain_time_max;

    //Copy previous row if same timestamp
    if (end_timestamp == start_timestamp) {
        if (!GetDB().WriteAnalytics(new_row)) {
            LogError("%s: Could not copy previous row\n", __func__);
            return false;
        }
    }

    int64_t request_start = start_timestamp * 1000; // ms
    int64_t request_end = end_timestamp * 1000;

    int64_t interval_ms = 60 * 1000;
    int64_t max_duration = 1000 * interval_ms;
    if (request_end - request_start > max_duration) {
        LogError("%s: Interval between blocks is too large for api\n", __func__);
        return false;
    }

    std::string url = std::string("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=") + std::to_string(request_start) + "&endTime=" + std::to_string(request_end);

    json response_json;
    if (!HttpGet(url, response_json)) {
        LogError("%s: Failed to fetch klines from Binance\n",__func__);
        return false;
    }


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
    AnalyticsRow new_row;
    if (!GetKlines(block, new_row)) {
        return false;
    }

    return false;
}

bool Ohlcvp::CustomInit(const std::optional<interfaces::BlockRef>& block)
{
    if (!block) {
        AnalyticsBatch out_batch;
        if (!LoadCsvToBatch("D:\\Code\\bitcoin\\ohlcvp.csv", out_batch)) {
            LogError("%s: Could not load backfill csv for ohlcvp\n", __func__);
            return false;
        }
        if (!GetDB().WriteAnalytics(out_batch)) {
            LogError("%s: Could not write backfill data to db for ohlcvp\n", __func__);
            return false;
        }
        auto last_height = out_batch.back().first;
        const CBlockIndex* locator_index{m_chainstate->m_blockman.LookupBlockIndex(m_chain->getBlockHash(last_height))};
        SetBestBlockIndex(locator_index);
        
    }
    return true;
}

BaseAnalytic::DB& Ohlcvp::GetDB() const { return *m_db; }
