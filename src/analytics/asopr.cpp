// Copyright (c) 2017-2022 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <analytics/asopr.h>

#include <clientversion.h>
#include <common/args.h>
#include <index/disktxpos.h>
#include <logging.h>
#include <node/blockstorage.h>
#include <validation.h>

constexpr uint8_t DB_TXTIMESTAMPINDEX{'t'};

std::unique_ptr<Asopr> g_asopr;


/** Access to the analytics database (analytics/) */
class Asopr::DB : public BaseAnalytic::DB
{
public:
    explicit DB(const fs::path& path);

    /// Read the disk location of the transaction data with the given hash. Returns false if the
    /// transaction hash is not indexed.
    //bool WriteAsopr(const std::pair<uint64_t,std::vector<double>>& vAsopr) const;

    /// Write a batch of transaction positions to the DB.
    [[nodiscard]] bool WriteAsopr(const std::pair<uint64_t,std::vector<double>>& vAsopr);
};

Asopr::DB::DB(const fs::path& path) :
    BaseAnalytics::DB(path)
{}
//gArgs.GetDataDirNet() "/analytics"


bool Asopr::DB::WriteAsopr(const std::pair<uint64_t,std::vector<double>>& vAsopr)
{
    return WriteAnalytics(vAsopr);
}

Asopr::Asopr(std::unique_ptr<interfaces::Chain> chain, const fs::path& path)
    : BaseAnalytic(std::move(chain), "asopr"), m_db(std::make_unique<Asopr::DB>(path))
{}

Asopr::~Asopr() = default;

bool Asopr::CustomAppend(const interfaces::BlockInfo& block)
{
    // Exclude genesis block transaction because outputs are not spendable.
    if (block.height == 0) return true;

    assert(block.data);



    uint64_t blockTime = block.data->GetBlockHeader().nTime;
    auto asopr = CalculateASOPR()

    std::pair<uint64_t,std::vector<double>> vAsopr std::make_pair(blockTime,std::vector<double>{asopr});

    return m_db->WriteAsopr(vAsopr);
}

BaseAnalytic::DB& Asopr::GetDB() const { return *m_db; }

double Asopr::CalculateASOPR(CBlock& block, CBlockUndo& blockUndo, const std::unordered_map<int64_t, double>& btc_price_map, std::ofstream& log_stream, std::ofstream& perf_stream) {
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
