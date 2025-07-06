#include "analytics.h"
#include <chain.h>
#include <core_io.h>
#include <validation.h>
#include <node/context.h>
#include <rpc/util.h>
#include <rpc/server.h>
#include <rpc/server_util.h>
#include <index/txtimestampindex.h>
#include <uint256.h>
#include <util/check.h>
#include <undo.h>

#include <net.h>
#include <net_processing.h>
#include <arrow/table.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/array.h>
#include <arrow/type.h>
#include <fstream>
#include <chrono>



using node::NodeContext;

std::optional<double> GetBTCPrice(const std::unordered_map<int64_t, double>& btc_price_map, 
                                  int64_t timestamp, 
                                  std::ofstream& log_stream) {  
    auto it = btc_price_map.find(timestamp);
    if (it != btc_price_map.end()) {
        return it->second;  // Found price, return it
    }

    // Log missing timestamp (efficient logging)
    if (log_stream.is_open()) {
        log_stream << timestamp << "\n";  // Append missing timestamp
    } else {
        std::cerr << "Error: log file is not open." << std::endl;
    }

    return std::nullopt;  // Indicate that no price was found
}

double CalculateASOPR(CBlock& block, CBlockUndo& blockUndo, const std::unordered_map<int64_t, double>& btc_price_map, std::ofstream& log_stream, std::ofstream& perf_stream) {
    double total_transaction_btc = 0;
    double total_created_usd = 0;
    uint64_t blocktime = block.GetBlockTime();

    for (CTransactionRef& tx : block.vtx) {
        if (tx->IsCoinBase()) {
            continue; // Skip coinbase transactions
        }
        CTxUndo* undoTX {nullptr};
        auto it = std::find_if(block.vtx.begin(), block.vtx.end(), [tx](CTransactionRef t){ return *t == *tx; });
        if (it != block.vtx.end()) {
            // -1 as blockundo does not have coinbase tx
            undoTX = &blockUndo.vtxundo.at(it - block.vtx.begin() - 1);
        }
        // Calculate total input value for the transaction
        for (unsigned int i = 0; i < tx->vin.size(); i++) {
            const CTxIn& txin = tx->vin[i];
            
            uint64_t vin_timestamp;
            g_txtimestampindex->GetTxTimestamp(txin.prevout.hash,vin_timestamp);
            if(blocktime - vin_timestamp < 3600){ //skip transactions shorter than an hour
                continue;
            }

            const Coin& prev_coin = undoTX->vprevout[i];
            const CTxOut& prev_txout = prev_coin.out;
            double btc_val = ValueFromAmount(prev_txout.nValue).get_real();
            total_transaction_btc += btc_val;
            auto price = GetBTCPrice(btc_price_map,vin_timestamp,log_stream);
            if (price){
                total_created_usd += btc_val * *price;
            }
            
        }
    }
    auto start = std::chrono::high_resolution_clock::now();
    auto block_price = GetBTCPrice(btc_price_map,blocktime,log_stream);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    perf_stream << "Timing of GetPrice: " << duration.count() <<"\n";

    // Avoid division by zero
    if (total_created_usd == 0 || !block_price) {
        if (log_stream.is_open()) {
            log_stream << "Total created: " + std::to_string(total_created_usd) << "\n";  // Append missing timestamp
        } else {
            std::cerr << "Error: log file is not open." << std::endl;
        }
        return 0.0;
    }


    return static_cast<double>(total_transaction_btc * *block_price) / total_created_usd;
}

std::unordered_map<int64_t, double> ConvertTableToMap(const std::shared_ptr<arrow::Table>& table) {
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


std::unordered_map<int64_t, double> LoadBTCPrices(const std::string& file_path){
    // Create a file reader
    auto file_result = arrow::io::ReadableFile::Open(file_path);
    if (!file_result.ok()) {
        std::ofstream log_file("D:/Code/bitcoin/error_log.txt", std::ios::app);
        log_file << "Error opening file: " << file_result.status().ToString() << std::endl;
        throw JSONRPCError(RPC_INTERNAL_ERROR, "Error creating logfile");
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
        throw JSONRPCError(RPC_INTERNAL_ERROR, "Error creating table reader: " + maybe_reader.status().ToString());
    }
    
    std::shared_ptr<arrow::csv::TableReader> reader  = *maybe_reader;

    // Read the table
    auto maybe_table = reader->Read();
   if (!maybe_table.ok()) {
        std::ofstream log_file("D:/Code\\bitcoin/error_log.txt", std::ios::app);
        log_file << "Error reading table: " << maybe_table.status().ToString() << std::endl;
        throw JSONRPCError(RPC_INTERNAL_ERROR, "Error reading table: " + maybe_table.status().ToString());
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
    /*
    // Print the table schema and number of rows
    std::ofstream log_file("D:/Code/bitcoin/output_log.txt", std::ios::app);
    log_file << "Table schema: " << (*maybe_table)->schema()->ToString() << std::endl;
    log_file << "Number of rows: " << (*maybe_table)->num_rows() << std::endl;

        // {{ edit_1 }}
    // Write the first 10 rows to a CSV file
    std::shared_ptr<arrow::Table> table = *maybe_table;
    std::shared_ptr<arrow::io::FileOutputStream> output_file;
    
    // Check if opening the output file was successful
    auto out_result = arrow::io::FileOutputStream::Open("D:/Code/bitcoin/first_10_rows.csv");
    if (!out_result.ok()) {
        std::ofstream log_file("D:/Code/bitcoin/error_log.txt", std::ios::app);
        log_file << "Error opening output file: " << out_result.status().ToString() << std::endl;
        return;
    }
    std::shared_ptr<arrow::io::OutputStream> output = *out_result;

    auto status = arrow::csv::WriteCSV(*table->Slice(0, 10), write_options, output.get());
    if (!status.ok()) {
        std::ofstream log_file("D:/Code/bitcoin/error_log.txt", std::ios::app);
        log_file << "Error writing to CSV: " << status.ToString() << std::endl; // Use status() to get the error
        return;
    }
    // {{ edit_1 }}

}*/

static const CBlockIndex* ParseHashOrHeight(const UniValue& param, ChainstateManager& chainman)
{
    LOCK(::cs_main);
    CChain& active_chain = chainman.ActiveChain();

    if (param.isNum()) {
        const int height{param.getInt<int>()};
        if (height < 0) {
            throw JSONRPCError(RPC_INVALID_PARAMETER, strprintf("Target block height %d is negative", height));
        }
        const int current_tip{active_chain.Height()};
        if (height > current_tip) {
            throw JSONRPCError(RPC_INVALID_PARAMETER, strprintf("Target block height %d after current tip %d", height, current_tip));
        }

        return active_chain[height];
    } else {
        const uint256 hash{ParseHashV(param, "hash_or_height")};
        const CBlockIndex* pindex = chainman.m_blockman.LookupBlockIndex(hash);

        if (!pindex) {
            throw JSONRPCError(RPC_INVALID_ADDRESS_OR_KEY, "Block not found");
        }

        return pindex;
    }
}

static RPCHelpMan gettransactiontimestamp()
{
    return RPCHelpMan{
                "gettransactiontimestamp",

                "Gets the timestamp of a transaction if -txtimestampindex is enabled.\n"

                "Returns the timestamp only.\n",
                {
                    {"txid", RPCArg::Type::STR_HEX, RPCArg::Optional::NO, "The transaction id"},
                },
                {
                    RPCResult{"if verbosity is not set or set to 0",
                         RPCResult::Type::NUM, "data", "The timestamp for 'txid'"
                     }
                },
                RPCExamples{
                    HelpExampleCli("gettransactiontimestamp", "\"mytxid\"")
                },
        [&](const RPCHelpMan& self, const JSONRPCRequest& request) -> UniValue
{
    const NodeContext& node = EnsureAnyNodeContext(request.context);
    ChainstateManager& chainman = EnsureChainman(node);

    uint256 hash = ParseHashV(request.params[0], "parameter 1");

    bool f_txtimestampindex_ready = false;
    if (g_txtimestampindex) {
        f_txtimestampindex_ready = g_txtimestampindex->BlockUntilSyncedToCurrentChain();
    }

    uint64_t timestamp;
    g_txtimestampindex->GetTxTimestamp(hash,timestamp);
    return timestamp;
},
    };
}

static RPCHelpMan getblockanalytics()
{
    return RPCHelpMan{
                "getblockanalytics",

                "Gets a set of analytics aggregated on block level.\n"

                "Returns all analytics directly.\n",
                {
                    {"hash_or_height", RPCArg::Type::NUM, RPCArg::Optional::NO, "The block hash or height of the target block",
                     RPCArgOptions{
                         .skip_type_check = true,
                         .type_str = {"", "string or numeric"},
                     }},
                },
                {
                    RPCResult{"if verbosity is not set or set to 0",
                         RPCResult::Type::NUM, "aSOPR", "The aggregated aSOPR for the block"
                     }
                },
                RPCExamples{
                    HelpExampleCli("getblockanalytics", "\"0000000000000000000076918decb6ddd61319731ffedbe6f92c7af75d55152d\"")
                },
        [&](const RPCHelpMan& self, const JSONRPCRequest& request) -> UniValue
{
    const NodeContext& node = EnsureAnyNodeContext(request.context);
    ChainstateManager& chainman = EnsureAnyChainman(request.context);
    const CBlockIndex& pindex{*CHECK_NONFATAL(ParseHashOrHeight(request.params[0], chainman))};

    uint256 hash = ParseHashV(request.params[0], "parameter 1");

    bool f_txtimestampindex_ready = false;
    if (g_txtimestampindex) {
        f_txtimestampindex_ready = g_txtimestampindex->BlockUntilSyncedToCurrentChain();
    }

    uint64_t timestamp;
    g_txtimestampindex->GetTxTimestamp(hash,timestamp);

    CBlockUndo blockUndo;
    CBlock block;

    if (!chainman.m_blockman.ReadBlockUndo(blockUndo, pindex)) {
        throw JSONRPCError(RPC_INTERNAL_ERROR, "Undo data expected but can't be read. This could be due to disk corruption or a conflict with a pruning event.");
    }
    if (!chainman.m_blockman.ReadBlock(block, pindex)) {
        throw JSONRPCError(RPC_INTERNAL_ERROR, "Block data expected but can't be read. This could be due to disk corruption or a conflict with a pruning event.");
    }

    std::string csv_file_path = "C:\\Users\\Oliver\\Code\\CryptoTrader\\data\\block_prices_usd.csv";
    auto btc_price_map = LoadBTCPrices(csv_file_path);

    std::ofstream log_stream("D:/Code/bitcoin/error_log.txt", std::ios::app);
    std::ofstream perf_stream("D:/Code/bitcoin/performance_log.txt", std::ios::app);
    auto start = std::chrono::high_resolution_clock::now();
    auto asopr = CalculateASOPR(block,blockUndo, btc_price_map,log_stream,perf_stream);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;

    
    if (perf_stream.is_open()) {
        perf_stream << "Timing of ASOPR: " << duration.count() <<"\n";  // Append missing timestamp
    } else {
        std::cerr << "Error: log file is not open." << std::endl;
    }

    return asopr;
},
    };
}

void RegisterAnalyticsRPCCommands(CRPCTable& t)
{
    static const CRPCCommand commands[]{
        {"analytics", &gettransactiontimestamp},
        {"analytics", &getblockanalytics},
    };
    for (const auto& c : commands) {
        t.appendCommand(c.name, &c);
    }
}