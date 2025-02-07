#include "analytics.h"
#include <chain.h>
#include <validation.h>
#include <node/context.h>
#include <rpc/util.h>
#include <rpc/server.h>
#include <rpc/server_util.h>
#include <index/txtimestampindex.h>
#include <uint256.h>
#include <util/check.h>

#include <net.h>
#include <net_processing.h>
#include <arrow/table.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <fstream>



using node::NodeContext;

double CalculateASOPR() {
    // Implement the logic to calculate aSOPR here
    double asopr = 0.0;
    // Calculation logic...
    return asopr;
}

void LoadBTCPrices(const std::string& file_path){
    // Create a file reader
    auto file_result = arrow::io::ReadableFile::Open(file_path);
    if (!file_result.ok()) {
        std::ofstream log_file("C:\\Users\\Oliver\Code\\bitcoin\\error_log.txt", std::ios::app);
        log_file << "Error opening file: " << file_result.status().ToString() << std::endl;
        return;
    }
    std::shared_ptr<arrow::io::ReadableFile> input_file = *file_result;

    // Create an IOContext
    arrow::io::IOContext io_context = arrow::io::default_io_context();

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    auto maybe_reader = arrow::csv::TableReader::Make(
        io_context, input_file, read_options, parse_options, convert_options);
    
    if (!maybe_reader.ok()) {
        std::ofstream log_file("C:\\Users\\Oliver\Code\\bitcoin\\error_log.txt", std::ios::app);
        log_file << "Error creating table reader: " << maybe_reader.status().ToString() << std::endl;
        return;
    }
    
    std::shared_ptr<arrow::csv::TableReader> reader  = *maybe_reader;

    // Read the table
    auto maybe_table = reader->Read();
   if (!maybe_table.ok()) {
        std::ofstream log_file("C:\\Users\\Oliver\Code\\bitcoin\\error_log.txt", std::ios::app);
        log_file << "Error reading table: " << maybe_table.status().ToString() << std::endl;
        return;
    }

    // Print the table schema and number of rows
    std::ofstream log_file("C:\\Users\\Oliver\Code\\bitcoin\\output_log.txt", std::ios::app);
    log_file << "Table schema: " << (*maybe_table)->schema()->ToString() << std::endl;
    log_file << "Number of rows: " << (*maybe_table)->num_rows() << std::endl;
}

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
                    HelpExampleCli("getblockanalytics", "\"00000000c937983704a73af28acdec37b049d214adbda81d7e2a3dd146f6ed09\"")
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

    std::string csv_file_path = "C:\\Users\\Oliver\\Code\\CryptoTrader\\data\\block_prices_usd.csv";
    LoadBTCPrices(csv_file_path);

    return timestamp;
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