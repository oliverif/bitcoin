// Copyright (c) 2017-2022 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <analytics/coreanalytics.h>
#include <analytics/ohlcvp.h>
#include <chainparams.h>
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
#include <index/coinstatsindex.h>
#include <kernel/coinstats.h>
#include <serialize.h>

std::unique_ptr<CoreAnalytics> g_coreanalytics;
using kernel::CCoinsStats;


/** Access to the analytics database (analytics/) */
class CoreAnalytics::DB : public BaseAnalytic::DB
{
public:
    explicit DB(const fs::path& path, std::string column_name);

    /// Read the disk location of the transaction data with the given hash. Returns false if the
    /// transaction hash is not indexed.
    //bool WriteCoreAnalytics(const std::pair<uint64_t,std::vector<double>>& vCoreAnalytics) const;

    /// Write a batch of transaction positions to the DB.
    [[nodiscard]] bool WriteCoreAnalytics(uint64_t height, const CoreAnalyticsRow& coreAnalyticsRow);
};

CoreAnalytics::DB::DB(const fs::path& path, std::string column_name) :
    BaseAnalytic::DB(
    StorageUtils::AnalyticStorageConfig{
        .analytic_id = "coreanalytics",
        .db_path = gArgs.GetDataDirNet() / "analytics" / "analytics.db ",
        .sqlite_db = nullptr,
        .table_name = "analytics",
        .columns = {
            {"height", "PRIMARY INTEGER"},
            {"issuance","REAL"},
            {"transaction_fees","REAL"},
            {"miner_revenue","REAL"},
            {"puell_multiple","REAL"},
            {"bdd","REAL"},
            {"asopr","REAL"},
            {"rc","REAL"},
            {"rp","REAL"},
            {"rl","REAL"},
            {"nrpl","REAL"},
            {"rplr","REAL"},
            {"cs","REAL"},
            {"mc","REAL"},
            {"mvrv","REAL"},
            {"mvrv_z","REAL"},
            {"realized_price","REAL"},
            {"rpv_ratio","REAL"},
            {"utxos_in_loss","REAL"},
            {"utxos_in_profit","REAL"},
            {"percent_utxos_in_profit","REAL"},
            {"percent_supply_in_profit","REAL"},
            {"total_supply_in_loss","REAL"},
            {"total_supply_in_profit","REAL"},
            {"ul","REAL"},
            {"up","REAL"},
            {"rul","REAL"},
            {"rup","REAL"},
            {"abdd","REAL"},
            {"vocd","REAL"},
            {"mvocd","REAL"},
            {"hodl_bank","REAL"},
            {"reserve_risk","REAL"},
            {"ath","REAL"},
            {"dfat", "REAL"}}
    })
{}

//TODO: Implement custom rewind

bool CoreAnalytics::DB::WriteCoreAnalytics(uint64_t height, const CoreAnalyticsRow& coreAnalyticsRow)
{
    std::vector<std::variant<int64_t, double>> vals = {
        coreAnalyticsRow.issuance,
        coreAnalyticsRow.transaction_fees,
        coreAnalyticsRow.miner_revenue,
        coreAnalyticsRow.puell_multiple,
        coreAnalyticsRow.bdd,
        coreAnalyticsRow.asopr,
        coreAnalyticsRow.rc,
        coreAnalyticsRow.rp,
        coreAnalyticsRow.rl,
        coreAnalyticsRow.nrpl,
        coreAnalyticsRow.rplr,
        coreAnalyticsRow.cs,
        coreAnalyticsRow.mc,
        coreAnalyticsRow.mvrv,
        coreAnalyticsRow.mvrv_z,
        coreAnalyticsRow.realized_price,
        coreAnalyticsRow.rpv_ratio,
        coreAnalyticsRow.utxos_in_loss,
        coreAnalyticsRow.utxos_in_profit,
        coreAnalyticsRow.percent_utxos_in_profit,
        coreAnalyticsRow.percent_supply_in_profit,
        coreAnalyticsRow.total_supply_in_loss,
        coreAnalyticsRow.total_supply_in_profit,
        coreAnalyticsRow.ul,
        coreAnalyticsRow.up,
        coreAnalyticsRow.rul,
        coreAnalyticsRow.rup,
        coreAnalyticsRow.abdd,
        coreAnalyticsRow.vocd,
        coreAnalyticsRow.mvocd,
        coreAnalyticsRow.hodl_bank,
        coreAnalyticsRow.reserve_risk,
        coreAnalyticsRow.ath,
        coreAnalyticsRow.dfath};
    AnalyticsRow row = {height, vals };
    return WriteAnalytics(row);
}

CoreAnalytics::CoreAnalytics(std::unique_ptr<interfaces::Chain> chain, const fs::path& path)
    : BaseAnalytic(std::move(chain), "coreanalytics"), m_db(std::make_unique<CoreAnalytics::DB>(path, "coreanalytics"))
{
    log_stream = std::ofstream("D:/Code/bitcoin/error_log.txt", std::ios::app);
    perf_stream = std::ofstream("D:/Code/bitcoin/performance_log.txt", std::ios::app);
}

CoreAnalytics::~CoreAnalytics() {
    perf_stream.close();
    log_stream.close();
    BaseAnalytic::~BaseAnalytic();
}

bool CoreAnalytics::CustomInit(const std::optional<interfaces::BlockRef>& block){
    // init previous vars
    if(!block.has_value()){
        return false;
    }
    //init genesis
    if(block.value().height == 0){
        m_temp_vars.previous_total_new_out = 0;
        m_temp_vars.previous_total_coinbase_amount = 0;
    }
    else{ //init otherwise
        const CBlockIndex* pindex = WITH_LOCK(cs_main, return m_chainstate->m_blockman.LookupBlockIndex(block.value().hash));
        const std::optional<CCoinsStats> maybe_coin_stats = g_coin_stats_index->LookUpStats(*pindex);
        if (!maybe_coin_stats.has_value()) {
            LogError("%s: Index data not yet available", __func__);
            return false;
        }
        const CCoinsStats& coin_stats = maybe_coin_stats.value();
        m_temp_vars.previous_total_new_out = coin_stats.total_new_outputs_ex_coinbase_amount;
        m_temp_vars.previous_total_coinbase_amount = coin_stats.total_coinbase_amount;
        m_temp_vars.previous_nTransactionOutputs = coin_stats.nTransactionOutputs;
     
        //get prev utxo map
        if (!LoadUtxoMap()) {
            return false;
        }

        //Note, the best_block loaded at init is the prev block. When sync is called p_next is the block processed. I.e the prev timestamp in the utxo map is the latest entry. 
        uint64_t start_height_year = GetHeightAfterTimestamp(m_utxo_map[block.value().height].timestamp - 31536000, block.value().height);
        std::vector<uint64_t> prev_stats_heights;
        for (auto i = start_height_year; i < block.value().height; i++) {
            prev_stats_heights.push_back(i);
        }
        AnalyticsBatch prev_stats;
        if (!GetDB().ReadAnalytics(prev_stats, {{"mvrv", "REAL"}, {"miner_rev", "REAL"}}, prev_stats_heights)) {
            LogError("s%: Couldn't read prev mvrv and miner_rev data from height s%", __func__, prev_stats_heights[0]);
            return false;
        }
        std::vector<double> mvrv_data;
        std::vector<double> miner_rev_data;
        for (auto row : prev_stats) {
            mvrv_data.push_back(std::get<double>(row.second[0]));
            miner_rev_data.push_back(std::get<double>(row.second[1]));
        }
        m_temp_vars.mvrv_stats.init_from_data(mvrv_data);
        m_temp_vars.miner_rev_stats.init_from_data(miner_rev_data);

        //Load vocd median
        uint64_t start_height_month = GetHeightAfterTimestamp(m_utxo_map[block.value().height].timestamp - 2592000, block.value().height);
        std::vector<uint64_t> prev_vocd_heights;
        for (auto i = start_height_month; i < block.value().height; i++) {
            prev_vocd_heights.push_back(i);
        }
        AnalyticsBatch prev_stats;
        if (!GetDB().ReadAnalytics(prev_stats, {{"vocd", "REAL"}}, prev_vocd_heights)) {
            LogError("s%: Couldn't read prev vocd from height s%", __func__, prev_vocd_heights[0]);
            return false;
        }
        std::vector<double> vocd_data;
        for (auto row : prev_stats) {
            vocd_data.push_back(std::get<double>(row.second[0]));
        }
        m_temp_vars.mvocd_running.init_from_data(vocd_data);

        AnalyticsRow prev_row;
        if (!GetDB().ReadAnalytics(prev_row, {{"ath", "REAL"}, {"hodl_bank", "REAL"}}, block.value().height)) {
            LogError("s%: Couldn't read prev vocd from height s%", __func__, prev_vocd_heights[0]);
            return false;
        }
        m_temp_vars.previous_ath = std::get<double>(prev_row.second[0]);
        m_temp_vars.preveious_hodl_bank = std::get<double>(prev_row.second[1]);
    }
    return true;
}

bool CoreAnalytics::LoadUtxoMap() {
    std::vector<uint8_t> blob;
    if (GetDB().ReadAnalyticsState(blob) && !blob.empty()) {
        try {
            std::vector<const uint8_t> const_blob(blob.begin(), blob.end());
            DataStream ds(const_blob);
            DeserializeUtxoMap(m_utxo_map, ds);
        } catch (const std::exception& e) {
            LogError("%s: Failed to deserialize UTXO map: %s\n", __func__, e.what());
            return false;
        }
    } else {
        LogError("%s: Failed to read UTXO map from db\n", __func__);
        return false;
    }
    return true;
}

bool CoreAnalytics::CustomAppend(const interfaces::BlockInfo& block)
{
    assert(block.data);
    if (prerequisite_height <= m_current_height) {
        if (!WaitForPrerequisite()) {
            return false;
        }
    }
    CBlockUndo block_undo;
    const CBlockIndex* pindex = WITH_LOCK(cs_main, return m_chainstate->m_blockman.LookupBlockIndex(block.hash));
    if (!m_chainstate->m_blockman.ReadBlockUndo(block_undo, *pindex)) {
        return false;
    }
    m_current_height = block.height;
    m_row.issuance = ValueFromAmount(GetBlockSubsidy(m_current_height, Params().GetConsensus())).get_real();

    if (!UpdatePriceMap()) {
        return false;
    }
    if (block.height == 0) {
        m_row.asopr = 1;
        m_row.rplr = 1;
        m_row.mvrv = 1;
        m_row.rpv_ratio = 1;
        m_row.rul = 1;
        m_row.rup = 1;
        m_temp_vars.mvocd_running.insert(0);
        m_row.mvocd = m_temp_vars.mvocd_running.median();
        m_temp_vars.mvrv_stats.add(0);
        m_temp_vars.miner_rev_stats.add(0);
    } else {
        // Skip this when genesis but keep metrics as 0 and write. Or we default some to 1
        if (!ProcessTransactions(block, block_undo)) {
            return false;
        }
        if (!GetIndexData(block, *pindex)) {
            return false;
        }
        if (!CalculateUtxoMetrics(block)) {
            return false;
        }
        m_row.mc = m_row.cs * m_current_price;
        m_utxo_map[block.height].utxo_count = m_temp_vars.delta_ntransaction_outputs + m_temp_vars.inputs;
        m_utxo_map[block.height].utxo_amount = m_temp_vars.spendable_out;
        m_row.rc = m_row.rc + m_temp_vars.spendable_out * m_current_price - m_temp_vars.previous_utxo_value;
        m_row.mvrv = m_row.mc / m_row.rc;
        m_row.realized_price = m_row.rc / m_row.cs;
        m_row.rpv_ratio = m_row.rp / m_row.rc;

        if (!UpdateMeanVars(block)) {
            return false;
        }

        m_row.rul = m_row.ul / m_row.mc;
        m_row.rup = m_row.up / m_row.mc;
        m_row.abdd = m_row.bdd / m_row.cs;
        m_row.vocd = m_row.abdd * m_current_price;

        if (!UpdateVocdMedian()) {
            return false;
        }
        m_row.hodl_bank = m_temp_vars.preveious_hodl_bank * m_current_price - m_row.mvocd;
        m_row.reserve_risk = m_current_price / m_row.hodl_bank;
    }

    if (!m_db->WriteCoreAnalytics(m_current_height,m_row)) {
        LogError("%s: Could not write new row for coreanalytics\n", __func__);
        return false;
    }

    return true;
}

bool CoreAnalytics::WaitForPrerequisite(std::chrono::seconds timeout = std::chrono::seconds(60))
{
    auto start = std::chrono::steady_clock::now();
    std::chrono::milliseconds delay(100);

    while (std::chrono::steady_clock::now() - start < timeout) {
        prerequisite_height = std::min(g_coin_stats_index->GetSummary().best_block_height, g_ohlcvp->GetSummary().best_block_height);
        if (prerequisite_height > m_current_height) {
            return true;
        }

        std::this_thread::sleep_for(delay);
        delay = std::min(delay * 2, std::chrono::milliseconds(5000)); // Exponential backoff
    }

    LogError("%s: Timed out while waiting for prerequisite analytics and index. Prerequisite at height %s\n", __func__, prerequisite_height);
    return false;
}

bool CoreAnalytics::CustomCommit()
{
    DataStream ds;
    SerializeUtxoMap(m_utxo_map, ds);
    std::string blob = ds.str(); // get raw bytes
    GetDB().WriteAnalyticsState({reinterpret_cast<const uint8_t*>(blob.data()),
                                 reinterpret_cast<const uint8_t*>(blob.data()) + blob.size()});
    return true;
}



BaseAnalytic::DB& CoreAnalytics::GetDB() const { return *m_db; }



void CoreAnalytics::SerializeUtxoMap(const UtxoMap& map, DataStream& s)
{
    s << static_cast<uint64_t>(map.size());
    for (const auto& [height, entry] : map) {
        s << height;
        entry.Serialize(s);
    }
}

void CoreAnalytics::DeserializeUtxoMap(UtxoMap& map, DataStream& s)
{
    map.clear();
    uint64_t size;
    s >> size;
    for (uint64_t i = 0; i < size; ++i) {
        int64_t height;
        s >> height;
        UtxoMapEntry entry;
        entry.Deserialize(s);
        map.emplace(height, std::move(entry));
    }
}

bool CoreAnalytics::UpdatePriceMap()
{
    //When genesis block, read row 0 of timestamp and price, else must check that current map is ok
    if (m_current_height > 0 && (m_utxo_map.empty() || !m_utxo_map.contains(m_current_height - 1))) {
        LogError("%s: Utxo map not loaded properly %s", __func__, m_current_height);
        return false;
    } else {
        AnalyticsRow new_row;
        if (!GetDB().ReadAnalytics(new_row, {{"timestamp", "INTEGER"}, {"price", "REAL"}}, m_current_height)) {
            LogError("%s: Could not read new price row of height %s", __func__, m_current_height);
            return false;
        }
        UtxoMapEntry new_entry{
            .timestamp = std::get<double>(new_row.second[0]),
            .price = std::get<double>(new_row.second[1]),
            .utxo_count = 0,
            .utxo_amount = 0};
        m_utxo_map[m_current_height] = new_entry;
        m_current_price = new_entry.price;
        m_current_timestamp = new_entry.timestamp;
        m_row.ath = std::max(m_row.ath, m_current_price);
        m_row.dfath = (m_current_price - m_row.ath) / m_row.ath;
    }

    return true;
}

bool CoreAnalytics::ProcessTransactions(const interfaces::BlockInfo& block, const CBlockUndo& blockUndo)
{
    m_row.bdd = 0;
    m_row.rp = 0;
    m_row.rl = 0;
    m_temp_vars.inputs = 0;
    m_temp_vars.previous_utxo_value = 0;
    double previous_adjusted_utxo_value = 0;
    double new_adjusted_utxo_amount = 0;

    for (const CTransactionRef& tx : block.data->vtx) {
        //We do not need to consider unspendables here, as we're only processing inputs, which are spend output and thereby implicitly spendable outputs.
        if (tx->IsCoinBase()) {
            continue; // Skip coinbase transactions as they don't have any inputs to consider. We only consider inputs in this function.
        }
        m_temp_vars.inputs += tx->vin.size();
        const CTxUndo* undoTX{ nullptr };
        auto it = std::find_if(block.data->vtx.begin(), block.data->vtx.end(), [tx](CTransactionRef t) { return *t == *tx; });
        if (it != block.data->vtx.end()) {
            // -1 as blockundo does not have coinbase tx
            undoTX = &blockUndo.vtxundo.at(it - block.data->vtx.begin() - 1);
        }
        // Calculate total input value for the transaction
        for (unsigned int i = 0; i < tx->vin.size(); i++) {
            const CTxIn& txin = tx->vin[i];
            const Coin& prev_coin = undoTX->vprevout[i];
            const CTxOut& prev_txout = prev_coin.out;
            double btc_amount = ValueFromAmount(prev_txout.nValue).get_real();

            auto it = m_utxo_map.find(prev_coin.nHeight);
            if (it != m_utxo_map.end()){
                LogError("%s: Price not available for block with height %s", __func__, prev_coin.nHeight);
                return false;
            }
            auto& utxo_entry = it->second;
            if (m_current_timestamp - utxo_entry.timestamp > 3600) { // skip transactions shorter than an hour
                previous_adjusted_utxo_value += btc_amount * utxo_entry.price;
                new_adjusted_utxo_amount += btc_amount;
            }
            m_temp_vars.previous_utxo_value += btc_amount * utxo_entry.price;

            m_row.bdd += btc_amount * static_cast<double>(m_current_timestamp - utxo_entry.timestamp) / 86400.0;

            if (m_current_price > utxo_entry.price) {
                m_row.rp += btc_amount * (m_current_price - utxo_entry.price);
            }
            else {
                m_row.rl += btc_amount * (utxo_entry.price - m_current_price);
            }

            utxo_entry.utxo_amount -= btc_amount;
            --utxo_entry.utxo_count;
           
        }
    }

    m_row.nrpl = m_row.rp - m_row.rl;
    m_row.rplr = m_row.rp / m_row.rl;
    m_row.asopr = new_adjusted_utxo_amount * m_current_price / previous_adjusted_utxo_value;

    return true;
}

bool CoreAnalytics::GetIndexData(const interfaces::BlockInfo& block, const CBlockIndex& block_index)
{
    const std::optional<CCoinsStats> maybe_coin_stats = g_coin_stats_index->LookUpStats(block_index);
    if (!maybe_coin_stats.has_value()) {
        LogError("%s: Index data not yet available", __func__);
        return false;
    }
    const CCoinsStats& coin_stats = maybe_coin_stats.value();
    m_temp_vars.spendable_out = ValueFromAmount(coin_stats.total_new_outputs_ex_coinbase_amount - m_temp_vars.previous_total_new_out + coin_stats.total_coinbase_amount - m_temp_vars.previous_total_coinbase_amount).get_real();
    m_temp_vars.coinbase_amount = ValueFromAmount(coin_stats.total_coinbase_amount - m_temp_vars.previous_total_coinbase_amount).get_real();

    m_temp_vars.previous_total_new_out = coin_stats.total_new_outputs_ex_coinbase_amount;
    m_temp_vars.previous_total_coinbase_amount = coin_stats.total_coinbase_amount;

    m_temp_vars.delta_ntransaction_outputs = coin_stats.nTransactionOutputs - m_temp_vars.previous_nTransactionOutputs;
    m_temp_vars.previous_nTransactionOutputs = coin_stats.nTransactionOutputs;

    m_row.miner_revenue = m_temp_vars.coinbase_amount * m_current_price;

    m_row.transaction_fees = m_temp_vars.coinbase_amount - m_row.issuance;

    if (!coin_stats.total_amount.has_value()) {
        LogError("s%: Total amount does not have value for height s%", __func__, m_current_height);
    }
    m_row.cs = ValueFromAmount(coin_stats.total_amount.value()).get_real();

    return true;
}

bool CoreAnalytics::CalculateUtxoMetrics(const interfaces::BlockInfo& block)
{
    m_row.ul = 0;
    m_row.up = 0;
    m_row.utxos_in_loss = 0;
    m_row.utxos_in_profit = 0;
    m_row.total_supply_in_loss = 0;
    m_row.total_supply_in_profit = 0;

    for (const auto& entry : m_utxo_map) {

        if (entry.second.utxo_amount == 0) {
            continue;
        }
        if (entry.second.price < m_current_price) {
            m_row.utxos_in_loss += entry.second.utxo_count;
            m_row.total_supply_in_loss += entry.second.utxo_amount;
            m_row.ul += entry.second.utxo_amount * entry.second.price;
        } else if (entry.second.price > m_current_price) {
            m_row.utxos_in_profit += entry.second.utxo_count;
            m_row.total_supply_in_profit += entry.second.utxo_amount;
            m_row.up += entry.second.utxo_amount * entry.second.price;
        }
    }

    return true;
}

bool CoreAnalytics::UpdateMeanVars(const interfaces::BlockInfo& block)
{
    auto start_height = GetHeightAfterTimestamp(m_current_timestamp - 31536000, m_current_height);
    auto prev_start_height = GetHeightAfterTimestamp(m_utxo_map[std::max((int)m_current_height - 1, 0)].timestamp - 31536000, m_current_height);
    std::vector<uint64_t> heights_to_remove;
    for (auto i = prev_start_height; i < start_height; i++) {
        heights_to_remove.push_back(i);
    }
    if (heights_to_remove.size() > 0) {
        AnalyticsBatch rows_to_remove;
        if (!GetDB().ReadAnalytics(rows_to_remove, {{"mvrv", "REAL"}, {"miner_rev", "REAL"}}, heights_to_remove)) {
            LogError("s%: Couldn't read prev mvrv and miner_rev data from height s%", __func__, heights_to_remove[0]);
            return false;
        }
        std::vector<double> mvrv_to_remove;
        std::vector<double> miner_rev_to_remove;
        for (auto row : rows_to_remove) {
            mvrv_to_remove.push_back(std::get<double>(row.second[0]));
            miner_rev_to_remove.push_back(std::get<double>(row.second[1]));
        }
        m_temp_vars.mvrv_stats.remove(mvrv_to_remove);
        m_temp_vars.miner_rev_stats.remove(miner_rev_to_remove);
    }
    m_temp_vars.mvrv_stats.add(m_row.mvrv);
    m_temp_vars.miner_rev_stats.add(m_row.miner_revenue);

    m_row.mvrv_z = (m_row.mvrv - m_temp_vars.mvrv_stats.mean) / std::sqrt(m_temp_vars.mvrv_stats.variance());
    m_row.puell_multiple = m_row.miner_revenue / m_temp_vars.miner_rev_stats.mean;

    return true;
}

uint64_t CoreAnalytics::GetHeightAfterTimestamp(uint64_t timestamp, uint64_t height)
{   
    if (height < 52560) {
        // Cannot go below zero or underflow
        return 0; // Or handle as needed
    }

    uint64_t start = height - 52560;
    int iteration_direction = 1;

    // Defensive: check if start exists in map
    if (m_utxo_map.find(start) == m_utxo_map.end()) {
        // Handle missing start key as you want
        return 0;
    }

    if (m_utxo_map[start].timestamp > timestamp) {
        iteration_direction = -1;
    } else {
        iteration_direction = 1;
    }

    uint64_t h = start;

    // Search upwards
    if (iteration_direction == 1) {
        while (h <= height) {
            auto it = m_utxo_map.find(h);
            if (it == m_utxo_map.end()) break; // no data here, stop or handle

            if (it->second.timestamp > timestamp) {
                return h;
            }
            h++;
        }
    }
    // Search downwards
    else {
        while (h > 0) {
            auto it = m_utxo_map.find(h);
            if (it == m_utxo_map.end()) break; // no data here, stop or handle

            if (it->second.timestamp <= timestamp) {
                return h + 1; // next height is just after timestamp
            }
            if (h == 0) break;
            h--;
        }
    }

    // If not found, return 0 or some sentinel
    return 0;
}

bool CoreAnalytics::UpdateVocdMedian()
{
    auto start_height = GetHeightAfterTimestamp(m_current_timestamp - 2592000, m_current_height);
    auto prev_start_height = GetHeightAfterTimestamp(m_utxo_map[std::max((int)m_current_height - 1,0)].timestamp - 2592000,m_current_height);
    std::vector<uint64_t> heights_to_remove;
    for (auto i = prev_start_height; i < start_height; i++) {
        heights_to_remove.push_back(i);
    }
    if (heights_to_remove.size() > 0) {
        AnalyticsBatch rows_to_remove;
        if (!GetDB().ReadAnalytics(rows_to_remove, {{"vocd", "REAL"}}, heights_to_remove)) {
            LogError("s%: Couldn't read prev mvrv and miner_rev data from height s%", __func__, heights_to_remove[0]);
            return false;
        }
        for (auto row : rows_to_remove) {
            m_temp_vars.mvocd_running.remove(std::get<double>(row.second[0]));
        }
    }
    m_temp_vars.mvocd_running.insert(m_row.vocd);
    m_row.mvocd = m_temp_vars.mvocd_running.median();
    return false;
}

void RunningStats::init_from_data(const std::vector<double>& data)
{
    n = data.size();
    if (n == 0) {
        mean = 0.0;
        M2 = 0.0;
        return;
    }

    // Compute mean
    double sum = 0.0;
    for (double x : data) {
        sum += x;
    }
    mean = sum / n;

    // Compute M2
    M2 = 0.0;
    for (double x : data) {
        double diff = x - mean;
        M2 += diff * diff;
    }
}

void RunningStats::add(double x)
{
    n++;
    double delta = x - mean;
    mean += delta / n;
    double delta2 = x - mean;
    M2 += delta * delta2;
}

void RunningStats::remove(const std::vector<double>& xs)
{
    for (double x : xs) {
        if (n <= 1) {
            n = 0;
            mean = 0.0;
            M2 = 0.0;
            return;
        }

        double delta = x - mean;
        n--;
        mean = (mean * (n + 1) - x) / n;
        double delta2 = x - mean;
        M2 -= delta * delta2;
    }
}

double RunningStats::variance() const
{
    return (n > 1) ? M2 / n : 0.0;
}
