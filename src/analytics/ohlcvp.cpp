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
