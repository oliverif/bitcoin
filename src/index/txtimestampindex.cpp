// Copyright (c) 2017-2022 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <index/txtimestampindex.h>

#include <clientversion.h>
#include <common/args.h>
#include <index/disktxpos.h>
#include <logging.h>
#include <node/blockstorage.h>
#include <validation.h>

constexpr uint8_t DB_TXTIMESTAMPINDEX{'t'};

std::unique_ptr<TxTimestampIndex> g_txtimestampindex;


/** Access to the txindex database (indexes/txindex/) */
class TxTimestampIndex::DB : public BaseIndex::DB
{
public:
    explicit DB(size_t n_cache_size, bool f_memory = false, bool f_wipe = false);

    /// Read the disk location of the transaction data with the given hash. Returns false if the
    /// transaction hash is not indexed.
    bool ReadTxTimestamp(const uint256& txid, uint64_t& timestamp) const;
    bool ReadTxTimestamps(CDBIterator& it, const std::vector<uint256>& txids, std::unordered_map<uint256, uint64_t, uint256Hasher>& timestamps) const;

    /// Write a batch of transaction positions to the DB.
    [[nodiscard]] bool WriteTxTimestamps(const std::vector<std::pair<uint256, uint64_t>>& vTimestamp);
};

TxTimestampIndex::DB::DB(size_t n_cache_size, bool f_memory, bool f_wipe) : BaseIndex::DB(gArgs.GetDataDirNet() / "indexes" / "txtimestampindex", n_cache_size, f_memory, f_wipe)
{
}

bool TxTimestampIndex::DB::ReadTxTimestamp(const uint256& txid, uint64_t& timestamp) const
{
    return Read(std::make_pair(DB_TXTIMESTAMPINDEX, txid), timestamp);
}

/* bool TxTimestampIndex::DB::ReadTxTimestamps(const std::vector<uint256>& txids, std::unordered_map<uint256, uint64_t, uint256Hasher>& timestamps) const
{
    CDBBatch batch(*this);
    for (const auto& txid : txids) {
        uint64_t timestamp;
        if (Read(std::make_pair(DB_TXTIMESTAMPINDEX, txid), timestamp)) {
            timestamps[txid] = timestamp;
        }
    }
    return true;
}*/

bool TxTimestampIndex::DB::ReadTxTimestamps(CDBIterator& it, const std::vector<uint256>& txids, std::unordered_map<uint256, uint64_t, uint256Hasher>& timestamps) const
{
    // std::unique_ptr<CDBIterator> it(NewIterator());
    // std::unique_ptr<CDBIterator> it(GetDB()->NewIterator());
    // CDBBatch batch(*this);

    // Sort txids for efficient iteration
    std::set<uint256> sorted_txids(txids.begin(), txids.end());

    for (const auto& txid : sorted_txids) {
        it.Seek(std::make_pair(DB_TXTIMESTAMPINDEX, txid));

        if (it.Valid()) {
            std::pair<uint8_t, uint256> key;
            uint64_t timestamp;
            if (it.GetKey(key) && key.first == DB_TXTIMESTAMPINDEX && key.second == txid && it.GetValue(timestamp)) {
                timestamps[txid] = timestamp;
            }
        }
    }
    return true;
}

bool TxTimestampIndex::DB::WriteTxTimestamps(const std::vector<std::pair<uint256, uint64_t>>& vTimestamp)
{
    CDBBatch batch(*this);
    for (const auto& tuple : vTimestamp) {
        batch.Write(std::make_pair(DB_TXTIMESTAMPINDEX, tuple.first), tuple.second);
    }
    return WriteBatch(batch);
}

TxTimestampIndex::TxTimestampIndex(std::unique_ptr<interfaces::Chain> chain, size_t n_cache_size, bool f_memory, bool f_wipe)
    : BaseIndex(std::move(chain), "txtimestampindex"), m_db(std::make_unique<TxTimestampIndex::DB>(n_cache_size, f_memory, f_wipe))
{
}

TxTimestampIndex::~TxTimestampIndex() = default;

bool TxTimestampIndex::CustomAppend(const interfaces::BlockInfo& block)
{
    // Exclude genesis block transaction because outputs are not spendable.
    if (block.height == 0) return true;

    assert(block.data);
    uint64_t blockTime = block.data->GetBlockHeader().nTime;
    std::vector<std::pair<uint256, uint64_t>> vTimestamp;
    vTimestamp.reserve(block.data->vtx.size());
    for (const auto& tx : block.data->vtx) {
        vTimestamp.emplace_back(tx->GetHash().ToUint256(), blockTime);
    }
    return m_db->WriteTxTimestamps(vTimestamp);
}

BaseIndex::DB& TxTimestampIndex::GetDB() const { return *m_db; }

bool TxTimestampIndex::GetTxTimestamp(const uint256& tx_hash, uint64_t& timestamp) const
{
    if (!m_db->ReadTxTimestamp(tx_hash, timestamp)) {
        return false;
    }
    return true;
}

bool TxTimestampIndex::GetTxTimestamps(const std::vector<uint256>& txids, std::unordered_map<uint256, uint64_t, uint256Hasher>& timestamps) const
{
    std::unique_ptr<CDBIterator> it(m_db->NewIterator());

    if (!m_db->ReadTxTimestamps(*it, txids, timestamps)) {
        return false;
    }
    return true;
}
