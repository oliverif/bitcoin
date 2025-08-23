// Copyright (c) 2017-present The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_ANALYTICS_BASE_H
#define BITCOIN_ANALYTICS_BASE_H

#include <analytics/storageutils.h>
#include <dbwrapper.h>
#include <interfaces/chain.h>
#include <interfaces/types.h>
#include <util/string.h>
#include <util/threadinterrupt.h>
#include <validationinterface.h>
#include <sqlite3.h>
#include <common/args.h>


#include <string>

class CBlock;
class CBlockIndex;
class Chainstate;
class ChainstateManager;
namespace interfaces {
class Chain;
} // namespace interfaces

using AnalyticsRow = std::pair<uint64_t, std::vector<std::variant<int64_t, double>>>;
using AnalyticsBatch = std::vector<AnalyticsRow>;

struct AnalyticSummary {
    std::string name;
    bool synced{false};
    int best_block_height{0};
    uint256 best_block_hash;
};

/**
 * Base class for analytics of blockchain data. This implements
 * CValidationInterface and ensures blocks are indexed sequentially according
 * to their position in the active chain.
 *
 * In the presence of multiple chainstates (i.e. if a UTXO snapshot is loaded),
 * only the background "IBD" chainstate will be indexed to avoid building the
 * index out of order. When the background chainstate completes validation, the
 * index will be reinitialized and indexing will continue.
 */
class BaseAnalytic : public CValidationInterface
{
protected:
    /**
     * The database stores a block locator of the chain the database is synced to
     * so that the index can efficiently determine the point it last stopped at.
     * A locator is used instead of a simple hash of the chain tip because blocks
     * and block index entries may not be flushed to disk until after this database
     * is updated.
    */
    class DB
    {
    private:
        StorageUtils::AnalyticStorageConfig storageConfig;
               
    public:
        DB(StorageUtils::AnalyticStorageConfig config);

        /// Read block locator of the chain that the index is in sync with.
        bool ReadBestBlock(CBlockLocator& locator) const;

        /// Write block locator of the chain that the index is in sync with.
        void WriteBestBlock(const CBlockLocator& locator);

        void WriteAnalyticsState(const std::vector<uint8_t>& state);
        bool ReadAnalyticsState(std::vector<uint8_t>& out_state);

        bool ReadAnalytics(AnalyticsRow& analytics, std::vector<StorageUtils::ColumnSpec> columns, uint64_t height) const;
        bool ReadAnalytics(AnalyticsBatch& analytics, std::vector<StorageUtils::ColumnSpec> columns, std::vector<uint64_t> heights) const;

        bool WriteAnalytics(const AnalyticsRow& analytics);
        bool WriteAnalytics(const AnalyticsBatch& analytics);
        const StorageUtils::AnalyticStorageConfig& GetStorageConfig() const;

        ~DB();
    };

private:
    /// Whether the analytics has been initialized or not.
    std::atomic<bool> m_init{false};
    /// Whether the analytics is in sync with the main chain. The flag is flipped
    /// from false to true once, after which point this starts processing
    /// ValidationInterface notifications to stay in sync.
    ///
    /// Note that this will latch to true *immediately* upon startup if
    /// `m_chainstate->m_chain` is empty, which will be the case upon startup
    /// with an empty datadir if, e.g., `-txindex=1` is specified.
    std::atomic<bool> m_synced{false};

    /// The last block in the chain that the analytics is in sync with.
    std::atomic<const CBlockIndex*> m_best_block_index{nullptr};

    std::thread m_thread_sync;
    CThreadInterrupt m_interrupt;

    /// Write the current analytics state (eg. chain block locator and subclass-specific items) to disk.
    ///
    /// Recommendations for error handling:
    /// If called on a successor of the previous committed best block in the analytics, the analytics can
    /// continue processing without risk of corruption, though the analytics state will need to catch up
    /// from further behind on reboot. If the new state is not a successor of the previous state (due
    /// to a chain reorganization), the analytics must halt until Commit succeeds or else it could end up
    /// getting corrupted.
    bool Commit();

    /// Loop over disconnected blocks and call CustomRewind.
    bool Rewind(const CBlockIndex* current_tip, const CBlockIndex* new_tip);

    virtual bool AllowPrune() const = 0;

    template <typename... Args>
    void FatalErrorf(util::ConstevalFormatString<sizeof...(Args)> fmt, const Args&... args);

    CBlockLocator GetLocator(interfaces::Chain& chain, const uint256& block_hash);
    
    const uint64_t start_timestamp{1382330735};

protected:
    std::unique_ptr<interfaces::Chain> m_chain;
    Chainstate* m_chainstate{nullptr};
    const std::string m_name;

    void BlockConnected(ChainstateRole role, const std::shared_ptr<const CBlock>& block, const CBlockIndex* pindex) override;

    void ChainStateFlushed(ChainstateRole role, const CBlockLocator& locator) override;

    /// Initialize internal state from the database and block index.
    [[nodiscard]] virtual bool CustomInit(const std::optional<interfaces::BlockRef>& block) { return true; }

    /// Write update analytics entries for a newly connected block.
    [[nodiscard]] virtual bool CustomAppend(const interfaces::BlockInfo& block) { return true; }

    /// Virtual method called internally by Commit that can be overridden to atomically
    /// commit more index state.
    virtual bool CustomCommit() { return true; }

    /// Rewind index to an earlier chain tip during a chain reorg. The tip must
    /// be an ancestor of the current best block.
    [[nodiscard]] virtual bool CustomRewind(const interfaces::BlockRef& current_tip, const interfaces::BlockRef& new_tip) { return true; }

    virtual DB& GetDB() const = 0;

    /// Update the internal best block index as well as the prune lock.
    void SetBestBlockIndex(const CBlockIndex* block);

public:
    BaseAnalytic(std::unique_ptr<interfaces::Chain> chain, std::string name);
    /// Destructor interrupts sync thread if running and blocks until it exits.
    virtual ~BaseAnalytic();

    /// Get the name of the analytics for display in logs.
    const std::string& GetName() const LIFETIMEBOUND { return m_name; }

    /// Blocks the current thread until the analytics is caught up to the current
    /// state of the block chain. This only blocks if the analytics has gotten in
    /// sync once and only needs to process blocks in the ValidationInterface
    /// queue. If the analytics is catching up from far behind, this method does
    /// not block and immediately returns false.
    bool BlockUntilSyncedToCurrentChain() const LOCKS_EXCLUDED(::cs_main);

    void Interrupt();

    /// Initializes the sync state and registers the instance to the
    /// validation interface so that it stays in sync with blockchain updates.
    [[nodiscard]] bool Init();

    /// Starts the initial sync process on a background thread.
    [[nodiscard]] bool StartBackgroundSync();

    /// Sync the analytics with the block index starting from the current best block.
    /// Intended to be run in its own thread, m_thread_sync, and can be
    /// interrupted with m_interrupt. Once the analytics gets in sync, the m_synced
    /// flag is set and the BlockConnected ValidationInterface callback takes
    /// over and the sync thread exits.
    void Sync();

    /// Stops the instance from staying in sync with blockchain updates.
    void Stop();

    /// Get a summary of the analytics and its state.
    AnalyticSummary GetSummary() const;
};

#endif // BITCOIN_ANALYTICS_BASE_H
