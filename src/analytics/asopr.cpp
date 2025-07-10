// Copyright (c) 2017-2022 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <analytics/asopr.h>

#include <clientversion.h>
#include <common/args.h>
#include <core_io.h>
#include <index/txtimestampindex.h>
#include <logging.h>
#include <node/blockstorage.h>
#include <validation.h>
#include <arrow/table.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/array.h>
#include <arrow/type.h>
#include <undo.h>
#include <univalue.h>



std::unique_ptr<Asopr> g_asopr;



/** Access to the analytics database (analytics/) */
class Asopr::DB : public BaseAnalytic::DB
{
public:
    explicit DB(const fs::path& path, std::string column_name);

    /// Read the disk location of the transaction data with the given hash. Returns false if the
    /// transaction hash is not indexed.
    //bool WriteAsopr(const std::pair<uint64_t,std::vector<double>>& vAsopr) const;

    /// Write a batch of transaction positions to the DB.
    [[nodiscard]] bool WriteAsopr(const AnalyticsRow& vAsopr);
};

Asopr::DB::DB(const fs::path& path, std::string column_name) :
    BaseAnalytic::DB(
    StorageUtils::AnalyticStorageConfig{
        .analytic_id = "asopr",
        .db_path = gArgs.GetDataDirNet() / "analytics" / "analytics.db ",
        .sqlite_db = nullptr,
        .table_name = "analytics",
        .columns = {{"height", "PRIMARY INTEGER"}, {"asopr","REAL"}},  
    })
{}
//gArgs.GetDataDirNet() "/analytics"



bool Asopr::DB::WriteAsopr(const AnalyticsRow& vAsopr)
{
    return WriteAnalytics(vAsopr);
}

Asopr::Asopr(std::unique_ptr<interfaces::Chain> chain, const fs::path& path)
    : BaseAnalytic(std::move(chain), "asopr"), m_db(std::make_unique<Asopr::DB>(path,"asopr"))
{
    std::string csv_file_path = "C:\\Users\\Oliver\\Code\\CryptoTrader\\data\\block_prices_usd.csv";
    btc_price_map = LoadBTCPrices(csv_file_path);

    log_stream = std::ofstream("D:/Code/bitcoin/error_log.txt", std::ios::app);
    perf_stream = std::ofstream("D:/Code/bitcoin/performance_log.txt", std::ios::app);
}

Asopr::~Asopr(){
    perf_stream.close();
    log_stream.close();
    BaseAnalytic::~BaseAnalytic();
}

bool Asopr::CustomAppend(const interfaces::BlockInfo& block)
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

    //point1 = std::chrono::high_resolution_clock::now();
    //std::chrono::duration<double> duration2 = point1 - point2;
    
     /* if (perf_stream.is_open()) {
        perf_stream << "Timing of Rest: " << duration2.count() <<"\n";
    } else {
        std::cerr << "Error: log file is not open." << std::endl;
    }*/

    auto asopr = CalculateASOPR(*block.data,block_undo, btc_price_map);
    /* point2 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = point2 - point1;
    if (perf_stream.is_open()) {
        perf_stream << "Timing of CalculateASOPR: " << duration.count() <<"\n";
    } else {
        std::cerr << "Error: log file is not open." << std::endl;
    }*/
    

    if (!asopr.has_value()) { return true;} //price missing, skip to next

    AnalyticsRow vAsopr = std::make_pair(blockTime, std::vector<std::variant<int64_t, double>>{asopr.value()}); 

    return m_db->WriteAsopr(vAsopr);
}

BaseAnalytic::DB& Asopr::GetDB() const { return *m_db; }

std::optional<double> Asopr::CalculateASOPR(const CBlock& block, const CBlockUndo& blockUndo, const std::unordered_map<int64_t, double>& btc_price_map) {
    double total_transaction_btc = 0;
    double total_created_usd = 0;
    uint64_t blocktime = block.GetBlockTime();
    bool priceMissing = false;
    CChain& cChain = m_chainstate->m_chainman.ActiveChain();

    for (const CTransactionRef& tx : block.vtx) {
        if (tx->IsCoinBase()) {
            continue; // Skip coinbase transactions
        }
        const CTxUndo* undoTX {nullptr};
        auto it = std::find_if(block.vtx.begin(), block.vtx.end(), [tx](CTransactionRef t){ return *t == *tx; });
        if (it != block.vtx.end()) {
            // -1 as blockundo does not have coinbase tx
            undoTX = &blockUndo.vtxundo.at(it - block.vtx.begin() - 1);
        }
        // Calculate total input value for the transaction
        for (unsigned int i = 0; i < tx->vin.size(); i++) {
            const CTxIn& txin = tx->vin[i];
            const Coin& prev_coin = undoTX->vprevout[i];
            uint64_t vin_timestamp = cChain[prev_coin.nHeight]->GetBlockTime();

            if(blocktime - vin_timestamp < 3600){ //skip transactions shorter than an hour
                continue;
            }

            
            const CTxOut& prev_txout = prev_coin.out;
            double btc_val = ValueFromAmount(prev_txout.nValue).get_real();
            total_transaction_btc += btc_val;
            auto price = GetBTCPrice(btc_price_map,vin_timestamp,log_stream);
            if (price){
                total_created_usd += btc_val * *price;
            }
            else{
                priceMissing = true;
            }
            
        }
    }
    auto block_price = GetBTCPrice(btc_price_map,blocktime,log_stream);
    if (!block_price || priceMissing){
        return std::nullopt;
    }

    // Avoid division by zero
    if (total_created_usd == 0) {
        if (log_stream.is_open()) {
            log_stream << "Total created: " + std::to_string(total_created_usd) << "\n";  // Append missing timestamp
        } else {
            std::cerr << "Error: log file is not open." << std::endl;
        }
        return 0.0;
    }


    return static_cast<double>(total_transaction_btc * *block_price) / total_created_usd;
}

std::unordered_map<int64_t, double> Asopr::LoadBTCPrices(const std::string& file_path){
    // Create a file reader
    auto file_result = arrow::io::ReadableFile::Open(file_path);
    if (!file_result.ok()) {
        std::ofstream log_file("D:/Code/bitcoin/error_log.txt", std::ios::app);
        log_file << "Error opening file: " << file_result.status().ToString() << std::endl;
        throw std::ios_base::failure("Error creating logfile");
    }
    std::shared_ptr<arrow::io::ReadableFile> input_file = *file_result;

    // Create an IOContext
    arrow::io::IOContext io_context = arrow::io::default_io_context();

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    auto write_options = arrow::csv::WriteOptions::Defaults();

    auto maybe_reader = arrow::csv::TableReader::Make(
        io_context, input_file, read_options, parse_options, convert_options);
    
    if (!maybe_reader.ok()) {
        std::ofstream log_file("D:/Code/bitcoin/error_log.txt", std::ios::app);
        log_file << "Error creating table reader: " << maybe_reader.status().ToString() << std::endl;
        throw std::ios_base::failure("Error creating table reader: " + maybe_reader.status().ToString());
    }
    
    std::shared_ptr<arrow::csv::TableReader> reader  = *maybe_reader;

    // Read the table
    auto maybe_table = reader->Read();
   if (!maybe_table.ok()) {
        std::ofstream log_file("D:/Code\\bitcoin/error_log.txt", std::ios::app);
        log_file << "Error reading table: " << maybe_table.status().ToString() << std::endl;
        throw std::ios_base::failure("Error reading table: " + maybe_table.status().ToString());
    }
    auto table = maybe_table.ValueOrDie();
    // Extract columns
    /*auto timestamp_column = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
    auto price_column = std::static_pointer_cast<arrow::DoubleArray>(table->column(1)->chunk(0));

    // Convert to unordered_map
    std::unordered_map<int64_t, double> btc_price_map;
    for (int64_t i = 0; i < timestamp_column->length(); ++i) {
        btc_price_map[timestamp_column->Value(i)] = price_column->Value(i);
    }*/
   auto btc_price_map = ConvertTableToMap(table);
    

    return btc_price_map;
}

std::optional<double> Asopr::GetBTCPrice(const std::unordered_map<int64_t, double>& btc_price_map, 
                                  int64_t timestamp, 
                                  std::ofstream& log_stream) {

    
    if (timestamp < 1382330735){return 165.0;}

    auto it = btc_price_map.find(timestamp);
    if (it != btc_price_map.end()) {
        return it->second;  // Found price, return it
    }

    // Log missing timestamp (efficient logging)
    if (log_stream.is_open()) {
        log_stream <<"Missing timestamp: " << timestamp << "\n";  // Append missing timestamp
    } else {
        std::cerr << "Error: log file is not open." << std::endl;
    }

    return std::nullopt;  // Indicate that no price was found
}

std::unordered_map<int64_t, double> Asopr::ConvertTableToMap(const std::shared_ptr<arrow::Table>& table) {
    auto timestamp_column = table->column(0);
    auto price_column = table->column(1);

    std::unordered_map<int64_t, double> btc_price_map;

    for (int chunk_index = 0; chunk_index < timestamp_column->num_chunks(); ++chunk_index) {
        auto timestamp_chunk = std::static_pointer_cast<arrow::Int64Array>(timestamp_column->chunk(chunk_index));
        auto price_chunk = std::static_pointer_cast<arrow::DoubleArray>(price_column->chunk(chunk_index));

        for (int64_t i = 0; i < timestamp_chunk->length(); ++i) {
            int64_t timestamp = timestamp_chunk->Value(i);
            double price = price_chunk->Value(i);
            btc_price_map[timestamp] = price;
        }
    }

    return btc_price_map;
}
