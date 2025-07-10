// Copyright (c) 2017-present The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <chainparams.h>
#include <common/args.h>
#include <analytics/baseanalytic.h>
#include <interfaces/chain.h>
#include <kernel/chain.h>
#include <logging.h>
#include <node/abort.h>
#include <node/blockstorage.h>
#include <node/context.h>
#include <node/database_args.h>
#include <node/interface_ui.h>
#include <tinyformat.h>
#include <util/string.h>
#include <util/thread.h>
#include <util/translation.h>
#include <validation.h> // For g_chainman


#include <string>
#include <utility>


constexpr auto SYNC_LOG_INTERVAL{30s};
constexpr auto SYNC_LOCATOR_WRITE_INTERVAL{30s};

template <typename... Args>
void BaseAnalytic::FatalErrorf(util::ConstevalFormatString<sizeof...(Args)> fmt, const Args&... args)
{
    auto message = tfm::format(fmt, args...);
    node::AbortNode(m_chain->context()->shutdown_request, m_chain->context()->exit_status, Untranslated(message), m_chain->context()->warnings.get());
}

CBlockLocator BaseAnalytic::GetLocator(interfaces::Chain& chain, const uint256& block_hash)
{
    CBlockLocator locator;
    bool found = chain.findBlock(block_hash, interfaces::FoundBlock().locator(locator));
    assert(found);
    assert(!locator.IsNull());
    return locator;
}

BaseAnalytic::DB::DB(StorageUtils::AnalyticStorageConfig config) : storageConfig(std::move(config))
{
    auto res = StorageUtils::ValidateConfig(storageConfig, true);
}

BaseAnalytic::DB::~DB() {
    if (storageConfig.sqlite_db) {
        sqlite3_close(storageConfig.sqlite_db);
        storageConfig.sqlite_db = nullptr;
    }

}

bool BaseAnalytic::DB::ReadBestBlock(CBlockLocator& locator) const
{
    const char* sql = "SELECT locator FROM sync_points WHERE analytic_id = ?;";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(storageConfig.sqlite_db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(storageConfig.sqlite_db) << std::endl;
        return false;
    }
    sqlite3_bind_text(stmt, 1, storageConfig.analytic_id.c_str(), -1, SQLITE_TRANSIENT); // Use GetAnalyticId() directly
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const void* blob = sqlite3_column_blob(stmt, 0);
        int blob_size = sqlite3_column_bytes(stmt, 0);
        if (!blob_size){
            return false;
            }
        // Read the blob into the DataStream
        std::vector<uint8_t> buffer(blob_size);
        std::memcpy(buffer.data(), blob, blob_size); // Copy blob data into buffer
        DataStream iss{std::move(buffer)}; // Initialize DataStream with the buffer
        iss >> locator; // Deserialize into locator

        sqlite3_finalize(stmt);
        return true;
    }
    sqlite3_finalize(stmt);
    return false;
}



void BaseAnalytic::DB::WriteBestBlock(const CBlockLocator& locator)
{
    // Serialize the CBlockLocator into a binary format
    DataStream oss;
    oss.reserve(1024);
    oss << locator; // Serialize the locator
    
    std::string locator_blob = oss.str(); // Get the serialized data as a string

    const char* sql = "INSERT OR REPLACE INTO sync_points (analytic_id, locator) VALUES (?, ?);";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(storageConfig.sqlite_db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(storageConfig.sqlite_db) << std::endl;
        return;
    }
    sqlite3_bind_text(stmt, 1, storageConfig.analytic_id.c_str(), -1, SQLITE_TRANSIENT); // Use GetAnalyticId() directly
    sqlite3_bind_blob(stmt, 2, locator_blob.data(), locator_blob.size(), SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        std::cerr << "Failed to execute statement: " << sqlite3_errmsg(storageConfig.sqlite_db) << std::endl;
        sqlite3_finalize(stmt);
        return;
    }
    sqlite3_finalize(stmt);
    return;
}

bool BaseAnalytic::DB::ReadAnalytics(AnalyticsRow& analytics, std::vector<StorageUtils::ColumnSpec> columns, uint64_t height) const
{
    AnalyticsBatch batch;
    if (!ReadAnalytics(batch, columns, {height})) {
        return false;
    }
    if (batch.empty()) {
        return false;
    }
    analytics = std::move(batch.at(0));
    return true;
}

bool BaseAnalytic::DB::ReadAnalytics(AnalyticsBatch& analytics, std::vector<StorageUtils::ColumnSpec> columns, std::vector<uint64_t> heights) const
{
    if (heights.empty() || columns.empty()) {
        return false;
    }

    // Build the SQL query
    std::string sql = "SELECT ";
    bool first = true;
    for (const auto& col : columns) {
        if (!first) sql += ", ";
        sql += col.name;
        first = false;
    }
    sql += " FROM " + storageConfig.table_name + " WHERE height IN (";
    for (size_t i = 0; i < heights.size(); ++i) {
        sql += (i == 0 ? "?" : ",?");
    }
    sql += ") ORDER BY height ASC;";

    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(storageConfig.sqlite_db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare statement: " + std::string(sqlite3_errmsg(storageConfig.sqlite_db)));
    }

    // Bind height values
    for (size_t i = 0; i < heights.size(); ++i) {
        sqlite3_bind_int64(stmt, static_cast<int>(i + 1), static_cast<sqlite3_int64>(heights[i]));
    }

    // Execute and collect rows
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        AnalyticsRow values;
        values.first = sqlite3_column_int64(stmt, 0);
        std::vector<std::variant<int64_t, double>> row_values;

        for (int col = 1; col <= static_cast<int>(columns.size()); ++col) {
            int type = sqlite3_column_type(stmt, col);
            if (type == SQLITE_INTEGER) {
                values.second.emplace_back(static_cast<int64_t>(sqlite3_column_int64(stmt, col)));
            } else if (type == SQLITE_FLOAT) {
                values.second.emplace_back(sqlite3_column_double(stmt, col));
            } else if (type == SQLITE_NULL) {
                values.second.emplace_back(int64_t(0)); // or std::nullopt if you support that
            } else {
                throw std::runtime_error("Unsupported column type in analytics table");
            }
        }

        analytics.emplace_back(values);
    }

    sqlite3_finalize(stmt);
    return true;
}

bool BaseAnalytic::DB::WriteAnalytics(const AnalyticsRow& analytics)
{
    return WriteAnalytics(AnalyticsBatch{analytics});
}

bool BaseAnalytic::DB::WriteAnalytics(const AnalyticsBatch& analytics)
{
    if (analytics.empty()) return true;
    // Construct the SQL statement
    std::string sql = "INSERT INTO " + storageConfig.table_name + " (";
    bool first = true;
    for (const auto& col : storageConfig.columns) {
        if (!first) sql += ", ";
        sql += col.name;
        first = false;
    }
    sql += ") VALUES (";
    for (size_t i = 0; i < storageConfig.columns.size(); ++i) {
        if (i > 0) sql += ", ";
        sql += "?";
    }
    sql += ");";

    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(storageConfig.sqlite_db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(storageConfig.sqlite_db) << std::endl;
        return false;
    }

    // Start transaction for batch insert
    char* errmsg = nullptr;
    if (sqlite3_exec(storageConfig.sqlite_db, "BEGIN TRANSACTION;", nullptr, nullptr, &errmsg) != SQLITE_OK) {
        std::cerr << "Failed to begin transaction: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        sqlite3_finalize(stmt);
        return false;
    }

    for (const auto& row : analytics) {
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        if (sqlite3_bind_int64(stmt, 1, row.first) != SQLITE_OK) {
            std::cerr << "Failed to bind height: " << sqlite3_errmsg(storageConfig.sqlite_db) << std::endl;
            sqlite3_finalize(stmt);
            return false;
        }

        for (int i = 0; i < row.second.size(); ++i) {
            auto sqlType = storageConfig.columns[i + 1].sqlite_type;
            auto& val = row.second.at(i);
            if (!StorageUtils::BindValue(stmt, static_cast<int>(i + 2), sqlType, row.second.at(i))) {
                auto errmsg = sqlite3_errmsg(storageConfig.sqlite_db);
                std::cerr << "Failed to bind value at index " << i << ": " << errmsg << std::endl;
                sqlite3_finalize(stmt);
                return false;
            }
        }

        if (sqlite3_step(stmt) != SQLITE_DONE) {
            std::cerr << "Failed to execute statement: " << sqlite3_errmsg(storageConfig.sqlite_db) << std::endl;
            sqlite3_finalize(stmt);
            return false;
        }
    }

    if (sqlite3_exec(storageConfig.sqlite_db, "END TRANSACTION;", nullptr, nullptr, &errmsg) != SQLITE_OK) {
        std::cerr << "Failed to end transaction: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        sqlite3_finalize(stmt);
        return false;
    }

    sqlite3_finalize(stmt);
    return true;
}

const StorageUtils::AnalyticStorageConfig& BaseAnalytic::DB::GetStorageConfig() const
{
    return storageConfig;
}

BaseAnalytic::BaseAnalytic(std::unique_ptr<interfaces::Chain> chain, std::string name)
    : m_chain{std::move(chain)}, m_name{std::move(name)} {}

BaseAnalytic::~BaseAnalytic()
{
    Interrupt();
    Stop();
}

static const uint256 GetBlockHashByDate(CChain& chain, const uint64_t& start_timestamp) {
    for (const CBlockIndex* pindex = chain.Genesis(); pindex; pindex = chain.Next(pindex)) {
        if (pindex->GetBlockTime() >= start_timestamp) {
            return pindex->GetBlockHash(); // Return the block hash
        }
    }
    return uint256(); // Return an empty hash if no block is found for the date
}



bool BaseAnalytic::Init()
{
    AssertLockNotHeld(cs_main);

    // May need reset if index is being restarted.
    m_interrupt.reset();

    // m_chainstate member gives indexing code access to node internals. It is
    // removed in followup https://github.com/bitcoin/bitcoin/pull/24230
    m_chainstate = WITH_LOCK(::cs_main,
        return &m_chain->context()->chainman->GetChainstateForIndexing());
    // Register to validation interface before setting the 'm_synced' flag, so that
    // callbacks are not missed once m_synced is true.
    m_chain->context()->validation_signals->RegisterValidationInterface(this);

    /* CBlockLocator locator;
    if (!GetDB().ReadBestBlock(locator)) {
        uint256 start_block_hash = GetBlockHashByDate(m_chainstate->m_chain, start_timestamp);
        locator = GetLocator(*m_chain,start_block_hash);
    }*/
    CBlockLocator locator;
    if (!GetDB().ReadBestBlock(locator)) {
        locator.SetNull();
    }

    LOCK(cs_main);
    CChain& index_chain = m_chainstate->m_chain;

    if (locator.IsNull()) {
        SetBestBlockIndex(nullptr);
    } else {
        // Setting the best block to the locator's top block. If it is not part of the
        // best chain, we will rewind to the fork point during index sync
        const CBlockIndex* locator_index{m_chainstate->m_blockman.LookupBlockIndex(locator.vHave.at(0))};
        if (!locator_index) {
            return InitError(Untranslated(strprintf("%s: best block of the analytics not found. Please rebuild the analytics.", GetName())));
        }
        SetBestBlockIndex(locator_index);
    }

    // Child init
    const CBlockIndex* start_block = m_best_block_index.load();
    if (!CustomInit(start_block ? std::make_optional(interfaces::BlockRef{start_block->GetBlockHash(), start_block->nHeight}) : std::nullopt)) {
        return false;
    }

    // Note: this will latch to true immediately if the user starts up with an empty
    // datadir and an index enabled. If this is the case, indexation will happen solely
    // via `BlockConnected` signals until, possibly, the next restart.
    m_synced = start_block == index_chain.Tip();
    m_init = true;
    return true;
}

static const CBlockIndex* NextSyncBlock(const CBlockIndex* pindex_prev, CChain& chain) EXCLUSIVE_LOCKS_REQUIRED(cs_main)
{
    AssertLockHeld(cs_main);

    if (!pindex_prev) {
        return chain.Genesis();
    }

    const CBlockIndex* pindex = chain.Next(pindex_prev);
    if (pindex) {
        return pindex;
    }

    return chain.Next(chain.FindFork(pindex_prev));
}

void BaseAnalytic::Sync()
{
    const CBlockIndex* pindex = m_best_block_index.load();
    if (!m_synced) {
        std::chrono::steady_clock::time_point last_log_time{0s};
        std::chrono::steady_clock::time_point last_locator_write_time{0s};
        while (true) {
            if (m_interrupt) {
                LogPrintf("%s: m_interrupt set; exiting ThreadSync\n", GetName());

                SetBestBlockIndex(pindex);
                // No need to handle errors in Commit. If it fails, the error will be already be
                // logged. The best way to recover is to continue, as index cannot be corrupted by
                // a missed commit to disk for an advanced index state.
                Commit();
                return;
            }

            const CBlockIndex* pindex_next = WITH_LOCK(cs_main, return NextSyncBlock(pindex, m_chainstate->m_chain));
            // If pindex_next is null, it means pindex is the chain tip, so
            // commit data indexed so far.
            if (!pindex_next) {
                SetBestBlockIndex(pindex);
                // No need to handle errors in Commit. See rationale above.
                Commit();

                // If pindex is still the chain tip after committing, exit the
                // sync loop. It is important for cs_main to be locked while
                // setting m_synced = true, otherwise a new block could be
                // attached while m_synced is still false, and it would not be
                // indexed.
                LOCK(::cs_main);
                pindex_next = NextSyncBlock(pindex, m_chainstate->m_chain);
                if (!pindex_next) {
                    m_synced = true;
                    break;
                }
            }
            if (pindex_next->pprev != pindex && !Rewind(pindex, pindex_next->pprev)) {
                FatalErrorf("%s: Failed to rewind index %s to a previous chain tip", __func__, GetName());
                return;
            }
            pindex = pindex_next;


            CBlock block;
            interfaces::BlockInfo block_info = kernel::MakeBlockInfo(pindex);
            if (!m_chainstate->m_blockman.ReadBlock(block, *pindex)) {
                FatalErrorf("%s: Failed to read block %s from disk",
                           __func__, pindex->GetBlockHash().ToString());
                return;
            } else {
                block_info.data = &block;
            }
            if (!CustomAppend(block_info)) {
                FatalErrorf("%s: Failed to write block %s to analytics database",
                           __func__, pindex->GetBlockHash().ToString());
                return;
            }

            auto current_time{std::chrono::steady_clock::now()};
            if (last_log_time + SYNC_LOG_INTERVAL < current_time) {
                LogPrintf("Syncing %s with block chain from height %d\n",
                          GetName(), pindex->nHeight);
                last_log_time = current_time;
            }

            if (last_locator_write_time + SYNC_LOCATOR_WRITE_INTERVAL < current_time) {
                SetBestBlockIndex(pindex);
                last_locator_write_time = current_time;
                // No need to handle errors in Commit. See rationale above.
                Commit();
            }
        }
    }

    if (pindex) {
        LogPrintf("%s is enabled at height %d\n", BaseAnalytic::GetName(), pindex->nHeight);
    } else {
        LogPrintf("%s is enabled\n", GetName());
    }
}

bool BaseAnalytic::Commit()
{
    // Don't commit anything if we haven't calculated analytic yet
    // (this could happen if init is interrupted).
    bool ok = m_best_block_index != nullptr;
    if (ok) {
        GetDB().WriteBestBlock(GetLocator(*m_chain, m_best_block_index.load()->GetBlockHash()));
    }
    if (!ok) {
        LogError("%s: Failed to commit latest %s state\n", __func__, GetName());
        return false;
    }
    return true;
}

bool BaseAnalytic::Rewind(const CBlockIndex* current_tip, const CBlockIndex* new_tip)
{
    assert(current_tip == m_best_block_index);
    assert(current_tip->GetAncestor(new_tip->nHeight) == new_tip);

    if (!CustomRewind({current_tip->GetBlockHash(), current_tip->nHeight}, {new_tip->GetBlockHash(), new_tip->nHeight})) {
        return false;
    }

    // In the case of a reorg, ensure persisted block locator is not stale.
    // Pruning has a minimum of 288 blocks-to-keep and getting the index
    // out of sync may be possible but a users fault.
    // In case we reorg beyond the pruned depth, ReadBlock would
    // throw and lead to a graceful shutdown
    SetBestBlockIndex(new_tip);
    if (!Commit()) {
        // If commit fails, revert the best block index to avoid corruption.
        SetBestBlockIndex(current_tip);
        return false;
    }

    return true;
}

void BaseAnalytic::BlockConnected(ChainstateRole role, const std::shared_ptr<const CBlock>& block, const CBlockIndex* pindex)
{
    // Ignore events from the assumed-valid chain; we will process its blocks
    // (sequentially) after it is fully verified by the background chainstate. This
    // is to avoid any out-of-order indexing.
    //
    // TODO at some point we could parameterize whether a particular index can be
    // built out of order, but for now just do the conservative simple thing.
    if (role == ChainstateRole::ASSUMEDVALID) {
        return;
    }

    // Ignore BlockConnected signals until we have fully indexed the chain.
    if (!m_synced) {
        return;
    }

    const CBlockIndex* best_block_index = m_best_block_index.load();
    if (!best_block_index) {
        if (pindex->nHeight != 0) {
            FatalErrorf("%s: First block connected is not the genesis block (height=%d)",
                       __func__, pindex->nHeight);
            return;
        }
    } else {
        // Ensure block connects to an ancestor of the current best block. This should be the case
        // most of the time, but may not be immediately after the sync thread catches up and sets
        // m_synced. Consider the case where there is a reorg and the blocks on the stale branch are
        // in the ValidationInterface queue backlog even after the sync thread has caught up to the
        // new chain tip. In this unlikely event, log a warning and let the queue clear.
        if (best_block_index->GetAncestor(pindex->nHeight - 1) != pindex->pprev) {
            LogPrintf("%s: WARNING: Block %s does not connect to an ancestor of "
                      "known best chain (tip=%s); not updating index\n",
                      __func__, pindex->GetBlockHash().ToString(),
                      best_block_index->GetBlockHash().ToString());
            return;
        }
        if (best_block_index != pindex->pprev && !Rewind(best_block_index, pindex->pprev)) {
            FatalErrorf("%s: Failed to rewind index %s to a previous chain tip",
                       __func__, GetName());
            return;
        }
    }
    interfaces::BlockInfo block_info = kernel::MakeBlockInfo(pindex, block.get());
    if (CustomAppend(block_info)) {
        // Setting the best block index is intentionally the last step of this
        // function, so BlockUntilSyncedToCurrentChain callers waiting for the
        // best block index to be updated can rely on the block being fully
        // processed, and the index object being safe to delete.
        SetBestBlockIndex(pindex);
    } else {
        FatalErrorf("%s: Failed to write block %s to analytics",
                   __func__, pindex->GetBlockHash().ToString());
        return;
    }
}

void BaseAnalytic::ChainStateFlushed(ChainstateRole role, const CBlockLocator& locator)
{
    // Ignore events from the assumed-valid chain; we will process its blocks
    // (sequentially) after it is fully verified by the background chainstate.
    if (role == ChainstateRole::ASSUMEDVALID) {
        return;
    }

    if (!m_synced) {
        return;
    }

    const uint256& locator_tip_hash = locator.vHave.front();
    const CBlockIndex* locator_tip_index;
    {
        LOCK(cs_main);
        locator_tip_index = m_chainstate->m_blockman.LookupBlockIndex(locator_tip_hash);
    }

    if (!locator_tip_index) {
        FatalErrorf("%s: First block (hash=%s) in locator was not found",
                   __func__, locator_tip_hash.ToString());
        return;
    }

    // This checks that ChainStateFlushed callbacks are received after BlockConnected. The check may fail
    // immediately after the sync thread catches up and sets m_synced. Consider the case where
    // there is a reorg and the blocks on the stale branch are in the ValidationInterface queue
    // backlog even after the sync thread has caught up to the new chain tip. In this unlikely
    // event, log a warning and let the queue clear.
    const CBlockIndex* best_block_index = m_best_block_index.load();
    if (best_block_index->GetAncestor(locator_tip_index->nHeight) != locator_tip_index) {
        LogPrintf("%s: WARNING: Locator contains block (hash=%s) not on known best "
                  "chain (tip=%s); not writing index locator\n",
                  __func__, locator_tip_hash.ToString(),
                  best_block_index->GetBlockHash().ToString());
        return;
    }

    // No need to handle errors in Commit. If it fails, the error will be already be logged. The
    // best way to recover is to continue, as index cannot be corrupted by a missed commit to disk
    // for an advanced index state.
    Commit();
}

bool BaseAnalytic::BlockUntilSyncedToCurrentChain() const
{
    AssertLockNotHeld(cs_main);

    if (!m_synced) {
        return false;
    }

    {
        // Skip the queue-draining stuff if we know we're caught up with
        // m_chain.Tip().
        LOCK(cs_main);
        const CBlockIndex* chain_tip = m_chainstate->m_chain.Tip();
        const CBlockIndex* best_block_index = m_best_block_index.load();
        if (best_block_index->GetAncestor(chain_tip->nHeight) == chain_tip) {
            return true;
        }
    }

    LogPrintf("%s: %s is catching up on block notifications\n", __func__, GetName());
    m_chain->context()->validation_signals->SyncWithValidationInterfaceQueue();
    return true;
}

void BaseAnalytic::Interrupt()
{
    m_interrupt();
}

bool BaseAnalytic::StartBackgroundSync()
{
    if (!m_init) throw std::logic_error("Error: Cannot start a non-initialized analytic");

    m_thread_sync = std::thread(&util::TraceThread, GetName(), [this] { Sync(); });
    return true;
}

void BaseAnalytic::Stop()
{
    if (m_chain->context()->validation_signals) {
        m_chain->context()->validation_signals->UnregisterValidationInterface(this);
    }

    if (m_thread_sync.joinable()) {
        m_thread_sync.join();
    }
}

AnalyticSummary BaseAnalytic::GetSummary() const
{
    AnalyticSummary summary{};
    summary.name = GetName();
    summary.synced = m_synced;
    if (const auto& pindex = m_best_block_index.load()) {
        summary.best_block_height = pindex->nHeight;
        summary.best_block_hash = pindex->GetBlockHash();
    } else {
        summary.best_block_height = 0;
        summary.best_block_hash = m_chain->getBlockHash(0);
    }
    return summary;
}

void BaseAnalytic::SetBestBlockIndex(const CBlockIndex* block)
{
    assert(!m_chainstate->m_blockman.IsPruneMode() || AllowPrune());

    if (AllowPrune() && block) {
        node::PruneLockInfo prune_lock;
        prune_lock.height_first = block->nHeight;
        WITH_LOCK(::cs_main, m_chainstate->m_blockman.UpdatePruneLock(GetName(), prune_lock));
    }

    // Intentionally set m_best_block_index as the last step in this function,
    // after updating prune locks above, and after making any other references
    // to *this, so the BlockUntilSyncedToCurrentChain function (which checks
    // m_best_block_index as an optimization) can be used to wait for the last
    // BlockConnected notification and safely assume that prune locks are
    // updated and that the index object is safe to delete.
    m_best_block_index = block;
}
