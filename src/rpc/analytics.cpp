#include "analytics.h"
#include <validation.h> // For accessing blockchain data
#include <node/context.h>
#include <rpc/util.h>
#include <rpc/server.h>
#include <rpc/server_util.h>
#include <index/txtimestampindex.h>
#include <uint256.h>

#include <net.h>
#include <net_processing.h>



using node::NodeContext;

double CalculateASOPR() {
    // Implement the logic to calculate aSOPR here
    double asopr = 0.0;
    // Calculation logic...
    return asopr;
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

void RegisterAnalyticsRPCCommands(CRPCTable& t)
{
    static const CRPCCommand commands[]{
        {"analytics", &gettransactiontimestamp},
    };
    for (const auto& c : commands) {
        t.appendCommand(c.name, &c);
    }
}