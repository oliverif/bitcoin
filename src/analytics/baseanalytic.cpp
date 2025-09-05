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
#include <undo.h>
#include <util/string.h>
#include <util/thread.h>
#include <util/translation.h>
#include <validation.h> // For g_chainman


#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
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
        LogError("%s: Failed to prepare statement: %s", __func__,sqlite3_errmsg(storageConfig.sqlite_db));
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



void BaseAnalytic::DB::WriteBestBlock(const CBlockLocator& locator, uint64_t height)
{
    // Serialize the CBlockLocator into a binary format
    DataStream oss;
    oss.reserve(1024);
    oss << locator; // Serialize the locator
    
    std::string locator_blob = oss.str(); // Get the serialized data as a string

    const char* sql = "INSERT OR REPLACE INTO sync_points (analytic_id, locator, height) VALUES (?, ?, ?);";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(storageConfig.sqlite_db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        LogError("%s: Failed to prepare statement: %s", __func__,sqlite3_errmsg(storageConfig.sqlite_db));
        return;
    }
    sqlite3_bind_text(stmt, 1, storageConfig.analytic_id.c_str(), -1, SQLITE_TRANSIENT); // Use GetAnalyticId() directly
    sqlite3_bind_blob(stmt, 2, locator_blob.data(), locator_blob.size(), SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 3, static_cast<sqlite3_int64>(height));
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        LogError("%s: Failed to execute statement: %s", __func__,sqlite3_errmsg(storageConfig.sqlite_db));
        sqlite3_finalize(stmt);
        return;
    }
    sqlite3_finalize(stmt);
    return;
}

// Write extra per-analytic state blob
void BaseAnalytic::DB::WriteAnalyticsState(const std::vector<uint8_t>& state)
{
    const char* sql =
        "UPDATE sync_points SET analytics_state = ? WHERE analytic_id = ?;";

    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(storageConfig.sqlite_db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        LogError("%s: Failed to prepare WriteAnalyticsState: %s",__func__,sqlite3_errmsg(storageConfig.sqlite_db));
        return;
    }

    if (!state.empty()) {
        sqlite3_bind_blob(stmt, 1, state.data(), state.size(), SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(stmt, 1);
    }

    sqlite3_bind_text(stmt, 2, storageConfig.analytic_id.c_str(), -1, SQLITE_TRANSIENT);

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        LogError("%s: Failed to execute WriteAnalyticsState: %s",__func__,sqlite3_errmsg(storageConfig.sqlite_db));
    }

    sqlite3_finalize(stmt);
}

// Read extra per-analytic state blob
bool BaseAnalytic::DB::ReadAnalyticsState(std::vector<uint8_t>& out_state)
{
    const char* sql =
        "SELECT analytics_state FROM sync_points WHERE analytic_id = ?;";

    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(storageConfig.sqlite_db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        LogError("%s: Failed to prepare ReadAnalyticsState: %s",__func__,sqlite3_errmsg(storageConfig.sqlite_db));
        return false;
    }

    sqlite3_bind_text(stmt, 1,storageConfig.analytic_id.c_str(), -1, SQLITE_TRANSIENT);

    bool ok = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const void* blob_data = sqlite3_column_blob(stmt, 0);
        int blob_size = sqlite3_column_bytes(stmt, 0);

        if (blob_data && blob_size > 0) {
            out_state.assign((const uint8_t*)blob_data,
                             (const uint8_t*)blob_data + blob_size);
            ok = true;
        }
    }

    sqlite3_finalize(stmt);
    return ok;
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

    StorageUtils::ColumnSpec target_name{"height", "INTEGER PRIMARY KEY"};
    auto it = std::find_if(columns.begin(), columns.end(),
                           [&target_name](const StorageUtils::ColumnSpec& col) {
                               return (col.name == target_name.name) && (col.sqlite_type == target_name.sqlite_type);
                           });

    if (it == columns.end()) {
        columns.insert(columns.begin(), {"height", "INTEGER PRIMARY KEY"});
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
        LogError("%s: Failed to prepare statement: %s",__func__, std::string(sqlite3_errmsg(storageConfig.sqlite_db)));
        return false;
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

        for (int col = 1; col < static_cast<int>(columns.size()); ++col) {
            int type = sqlite3_column_type(stmt, col);
            if (type == SQLITE_INTEGER) {
                values.second.emplace_back(static_cast<int64_t>(sqlite3_column_int64(stmt, col)));
            } else if (type == SQLITE_FLOAT) {
                values.second.emplace_back(sqlite3_column_double(stmt, col));
            } else if (type == SQLITE_NULL) {
                values.second.emplace_back(int64_t(0)); // or std::nullopt if you support that
            } else {
                LogError("%s: Unsupported column type in analytics table",__func__);
                return false;
            }
        }

        analytics.emplace_back(values);
    }

    sqlite3_finalize(stmt);
    return true;
}

bool BaseAnalytic::DB::ReadAnalytics(AnalyticsBatch& analytics, std::vector<StorageUtils::ColumnSpec> columns, uint64_t from_height, uint64_t to_height) const
{
    StorageUtils::ColumnSpec target_name{"height", "INTEGER PRIMARY KEY"};
    auto it = std::find_if(columns.begin(), columns.end(),
                           [&target_name](const StorageUtils::ColumnSpec& col) {
                               return (col.name == target_name.name) && (col.sqlite_type == target_name.sqlite_type);
                           });

    if (it == columns.end()) {
        columns.insert(columns.begin(), {"height", "INTEGER PRIMARY KEY"});
    }
    // Build the SQL query
    std::string sql = "SELECT ";
    bool first = true;
    for (const auto& col : columns) {
        if (!first) sql += ", ";
        sql += col.name;
        first = false;
    }
    sql += " FROM " + storageConfig.table_name + " WHERE height BETWEEN ? AND ? ORDER BY height ASC;";

    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(storageConfig.sqlite_db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        LogError("%s: Failed to prepare statement: %s", __func__, std::string(sqlite3_errmsg(storageConfig.sqlite_db)));
        return false;
    }

    // Bind height values
    sqlite3_bind_int64(stmt, static_cast<int>(1), static_cast<sqlite3_int64>(from_height));
    sqlite3_bind_int64(stmt, static_cast<int>(2), static_cast<sqlite3_int64>(to_height));

    // Execute and collect rows
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        AnalyticsRow values;
        values.first = sqlite3_column_int64(stmt, 0);
        std::vector<std::variant<int64_t, double>> row_values;

        for (int col = 1; col < static_cast<int>(columns.size()); ++col) {
            int type = sqlite3_column_type(stmt, col);
            if (type == SQLITE_INTEGER) {
                values.second.emplace_back(static_cast<int64_t>(sqlite3_column_int64(stmt, col)));
            } else if (type == SQLITE_FLOAT) {
                values.second.emplace_back(sqlite3_column_double(stmt, col));
            } else if (type == SQLITE_NULL) {
                values.second.emplace_back(int64_t(0)); // or std::nullopt if you support that
            } else {
                LogError("%s: Unsupported column type in analytics table", __func__);
                return false;
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
    sql += ") ON CONFLICT(";

    // If height exist must update instead of replacing
    sql += storageConfig.columns[0].name;
    sql += ") DO UPDATE SET ";

    first = true;
    for (size_t i = 1; i < storageConfig.columns.size(); ++i) {
        if (!first) sql += ", ";
        sql += storageConfig.columns[i].name + " = excluded." + storageConfig.columns[i].name;
        first = false;
    }
    sql += ";";

    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(storageConfig.sqlite_db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        LogError("%s: Failed to prepare statement: %s\n",__func__, sqlite3_errmsg(storageConfig.sqlite_db));
        return false;
    }

    // Start transaction for batch insert
    char* errmsg = nullptr;
    if (sqlite3_exec(storageConfig.sqlite_db, "BEGIN TRANSACTION;", nullptr, nullptr, &errmsg) != SQLITE_OK) {
        LogError("%s: Failed to begin transaction: %s\n",__func__,errmsg);
        sqlite3_free(errmsg);
        sqlite3_finalize(stmt);
        return false;
    }

    for (const auto& row : analytics) {
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);

        if (sqlite3_bind_int64(stmt, 1, row.first) != SQLITE_OK) {
            LogError("%s: Failed to bind height: %s\n",__func__, sqlite3_errmsg(storageConfig.sqlite_db));
            sqlite3_finalize(stmt);
            return false;
        }

        for (int i = 0; i < row.second.size(); ++i) {
            auto sqlType = storageConfig.columns[i + 1].sqlite_type;
            if (!StorageUtils::BindValue(stmt, static_cast<int>(i + 2), sqlType, row.second.at(i))) {
                auto errmsg = sqlite3_errmsg(storageConfig.sqlite_db);
                LogError("%s: Failed to bind value at index %s: %s\n",__func__,std::to_string(i),errmsg);
                sqlite3_finalize(stmt);
                return false;
            }
        }

        if (sqlite3_step(stmt) != SQLITE_DONE) {
            LogError("%s: Failed to execute statement: %s\n",__func__,sqlite3_errmsg(storageConfig.sqlite_db));
            sqlite3_finalize(stmt);
            return false;
        }
    }

    if (sqlite3_exec(storageConfig.sqlite_db, "END TRANSACTION;", nullptr, nullptr, &errmsg) != SQLITE_OK) {
        LogError("%s: Failed to end transaction: %s\n",__func__,errmsg);
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

bool BaseAnalytic::ProcessBlock(const CBlockIndex* pindex, const CBlock* block_data)
{
    interfaces::BlockInfo block_info = kernel::MakeBlockInfo(pindex, block_data);

    CBlock block;
    if (!block_data) { // disk lookup if block data wasn't provided
        if (!m_chainstate->m_blockman.ReadBlock(block, *pindex)) {
            FatalErrorf("Failed to read block %s from disk",
                        pindex->GetBlockHash().ToString());
            return false;
        }
        block_info.data = &block;
    }

    CBlockUndo block_undo;
    if (CustomOptions().connect_undo_data) {
        if (pindex->nHeight > 0 && !m_chainstate->m_blockman.ReadBlockUndo(block_undo, *pindex)) {
            FatalErrorf("Failed to read undo block data %s from disk",
                        pindex->GetBlockHash().ToString());
            return false;
        }
        block_info.undo_data = &block_undo;
    }

    if (!CustomAppend(block_info)) {
        FatalErrorf("Failed to write block %s to analytics database",
                    pindex->nHeight);
        return false;
    }

    return true;
}

void BaseAnalytic::Sync()
{
    const CBlockIndex* pindex = m_best_block_index.load();
    if (!m_synced) {
        auto last_log_time{NodeClock::now()};
        auto last_locator_write_time{last_log_time};
        while (true) {
            if (m_interrupt) {
                LogInfo("%s: m_interrupt set; exiting ThreadSync", GetName());

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
                FatalErrorf("Failed to rewind %s to a previous chain tip", GetName());
                return;
            }
            pindex = pindex_next;


            if (!ProcessBlock(pindex)) return; // error logged internally

            auto current_time{NodeClock::now()};
            if (current_time - last_log_time >= SYNC_LOG_INTERVAL) {
                LogInfo("Syncing %s with block chain from height %d", GetName(), pindex->nHeight);
                last_log_time = current_time;
            }

            if (current_time - last_locator_write_time >= SYNC_LOCATOR_WRITE_INTERVAL) {
                SetBestBlockIndex(pindex);
                last_locator_write_time = current_time;
                // No need to handle errors in Commit. See rationale above.
                Commit();
            }
        }
    }

    if (pindex) {
        LogInfo("%s is enabled at height %d", GetName(), pindex->nHeight);
    } else {
        LogInfo("%s is enabled", GetName());
    }
}

bool BaseAnalytic::Commit()
{
    // Don't commit anything if we haven't calculated analytic yet
    // (this could happen if init is interrupted).
    bool ok = m_best_block_index != nullptr;
    if (ok) {
        CBlockLocator prev_commited;
        if (!GetDB().ReadBestBlock(prev_commited)) {
            prev_commited.SetNull();
        }
        GetDB().WriteBestBlock(GetLocator(*m_chain, m_best_block_index.load()->GetBlockHash()), m_best_block_index.load()->nHeight);
        if (!CustomCommit()) {
            //Rewind best block if CustomCommit fails
            const CBlockIndex* prev_locator_index{m_chainstate->m_blockman.LookupBlockIndex(prev_commited.vHave.at(0))};
            GetDB().WriteBestBlock(prev_commited, prev_locator_index->nHeight);
            return false;
        }
    }
    if (!ok) {
        LogError("%s: Failed to commit latest %s state\n", __func__, GetName());
        return false;
    }
    return true;
}

bool BaseAnalytic::Rewind(const CBlockIndex* current_tip, const CBlockIndex* new_tip)
{
    assert(current_tip->GetAncestor(new_tip->nHeight) == new_tip);

    CBlock block;
    CBlockUndo block_undo;

    for (const CBlockIndex* iter_tip = current_tip; iter_tip != new_tip; iter_tip = iter_tip->pprev) {
        interfaces::BlockInfo block_info = kernel::MakeBlockInfo(iter_tip);
        if (CustomOptions().disconnect_data) {
            if (!m_chainstate->m_blockman.ReadBlock(block, *iter_tip)) {
                LogError("Failed to read block %s from disk",
                         iter_tip->GetBlockHash().ToString());
                return false;
            }
            block_info.data = &block;
        }
        if (CustomOptions().disconnect_undo_data && iter_tip->nHeight > 0) {
            if (!m_chainstate->m_blockman.ReadBlockUndo(block_undo, *iter_tip)) {
                return false;
            }
            block_info.undo_data = &block_undo;
        }
        if (!CustomRemove(block_info)) {
            return false;
        }
    }

    // Don't commit here - the committed index state must never be ahead of the
    // flushed chainstate, otherwise unclean restarts would lead to index corruption.
    // Pruning has a minimum of 288 blocks-to-keep and getting the index
    // out of sync may be possible but a users fault.
    // In case we reorg beyond the pruned depth, ReadBlock would
    // throw and lead to a graceful shutdown
    SetBestBlockIndex(new_tip);
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
            FatalErrorf("First block connected is not the genesis block (height=%d)",
                        pindex->nHeight);
            return;
        }
    } else {
        // Ensure block connects to an ancestor of the current best block. This should be the case
        // most of the time, but may not be immediately after the sync thread catches up and sets
        // m_synced. Consider the case where there is a reorg and the blocks on the stale branch are
        // in the ValidationInterface queue backlog even after the sync thread has caught up to the
        // new chain tip. In this unlikely event, log a warning and let the queue clear.
        if (best_block_index->GetAncestor(pindex->nHeight - 1) != pindex->pprev) {
            LogWarning("Block %s does not connect to an ancestor of "
                       "known best chain (tip=%s); not updating index",
                       pindex->GetBlockHash().ToString(),
                       best_block_index->GetBlockHash().ToString());
            return;
        }
        if (best_block_index != pindex->pprev && !Rewind(best_block_index, pindex->pprev)) {
            FatalErrorf("Failed to rewind %s to a previous chain tip",
                        GetName());
            return;
        }
    }

    // Dispatch block to child class; errors are logged internally and abort the node.
    if (ProcessBlock(pindex, block.get())) {
        // Setting the best block index is intentionally the last step of this
        // function, so BlockUntilSyncedToCurrentChain callers waiting for the
        // best block index to be updated can rely on the block being fully
        // processed, and the index object being safe to delete.
        SetBestBlockIndex(pindex);
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
