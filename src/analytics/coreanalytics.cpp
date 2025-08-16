// Copyright (c) 2017-2022 The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <analytics/coreanalytics.h>
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
    [[nodiscard]] bool WriteCoreAnalytics(const CoreAnalyticsRow& coreAnalyticsRow);
};

CoreAnalytics::DB::DB(const fs::path& path, std::string column_name) :
    BaseAnalytic::DB(
    StorageUtils::AnalyticStorageConfig{
        .analytic_id = "coreanalytics",
        .db_path = gArgs.GetDataDirNet() / "analytics" / "analytics.db ",
        .sqlite_db = nullptr,
        .table_name = "analytics",
        .columns = {{"height", "PRIMARY INTEGER"}, {"coreanalytics","REAL"}},  
    })
{}
//gArgs.GetDataDirNet() "/analytics"



bool CoreAnalytics::DB::WriteCoreAnalytics(const CoreAnalyticsRow& coreAnalyticsRow)
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
            AnalyticsRow row = { coreAnalyticsRow.height, vals };
            return WriteAnalytics(row);
}

CoreAnalytics::CoreAnalytics(std::unique_ptr<interfaces::Chain> chain, const fs::path& path)
    : BaseAnalytic(std::move(chain), "coreanalytics"), m_db(std::make_unique<CoreAnalytics::DB>(path, "coreanalytics"))
{
    std::string csv_file_path = "C:\\Users\\Oliver\\Code\\CryptoTrader\\data\\block_prices_usd.csv";
    btc_price_map = LoadBTCPrices(csv_file_path);

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
        const std::optional<CCoinsStats> maybe_coin_stats = g_coin_stats_index->LookUpStats(block_index);
        if (!maybe_coin_stats.has_value()) {
            LogError("%s: Index data not yet available", __func__);
            return false;
        }
        const CCoinsStats& coin_stats = maybe_coin_stats.value();
    }
}

bool CoreAnalytics::CustomAppend(const interfaces::BlockInfo& block)
{
    //TODO: add logic to make this analytic wait for ohlcvp
    // Exclude genesis block transaction because outputs are not spendable.
    if (block.height == 0) return true;

    assert(block.data);

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
    m_row.realized_price = m_row.rc/m_row.cs;
    m_row.rpv_ratio = m_row.rp/m_row.rc;

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
    m_row.reserve_risk = m_current_price/m_row.hodl_bank;

    AnalyticsRow vCoreAnalytics = std::make_pair(blockTime, std::vector<std::variant<int64_t, double>>{coreanalytics.value()});

    return m_db->WriteCoreAnalytics(vCoreAnalytics);
}

BaseAnalytic::DB& CoreAnalytics::GetDB() const { return *m_db; }

bool CoreAnalytics::UpdatePriceMap()
{
    if (m_utxo_map.empty()) {
        // Get price from db
        // and set ath to last
    } else {
        AnalyticsRow new_row;
        if (!GetDB().ReadAnalytics(new_row, {{"timestamp", "INTEGER"}, {"price", "REAL"}}, m_current_height)) {
            LogError("%s: Could not read new price row of height %s", __func__, m_current_height);
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
    //TODO: Add calculations for the other stuff that needs to be calculated in the loop. Change name of function
    m_row.bdd = 0;
    m_row.rp = 0;
    m_row.rl = 0;
    m_temp_vars.inputs = 0;
    m_temp_vars.previous_utxo_value = 0;
    double previous_adjusted_utxo_value = 0;
    double new_adjusted_utxo_amount = 0;
    double young_coins = 0;
    uint64_t current_timestamp;
    double current_price;
    {
        auto it = m_utxo_map.find(block.height);
        if (it != m_utxo_map.end()) {
            LogError("%s: Price not available for block with height %s", __func__, prev_coin.nHeight);
            return false;
        }
        current_timestamp = it->second.timestamp;
        current_price = it->second.price;
    }


    for (const CTransactionRef& tx : block.data->vtx) {
        if (tx->IsCoinBase()) {
            continue; // Skip coinbase transactions
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
            if (current_timestamp - utxo_entry.timestamp > 3600) { // skip transactions shorter than an hour
                previous_adjusted_utxo_value += btc_amount * utxo_entry.price;
                new_adjusted_utxo_amount += btc_amount;
            }
            m_temp_vars.previous_utxo_value += btc_amount * utxo_entry.price;

            m_row.bdd += btc_amount * static_cast<double>(current_timestamp - utxo_entry.timestamp) / 86400.0;

            if (current_price > utxo_entry.price) {
                m_row.rp += btc_amount * (current_price - utxo_entry.price);
            }
            else {
                m_row.rl += btc_amount * (utxo_entry.price - current_price);
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
    auto start_height = GetHeightAfterTimestamp(m_current_timestamp - 31536000);
    auto prev_start_height = GetHeightAfterTimestamp(m_utxo_map[std::max((int)m_current_height - 1,0)].timestamp - 31536000);
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

uint64_t CoreAnalytics::GetHeightAfterTimestamp(uint64_t timestamp)
{   
    if (m_current_height < 52560) {
        // Cannot go below zero or underflow
        return 0; // Or handle as needed
    }

    uint64_t start = m_current_height - 52560;
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

    uint64_t height = start;

    // Search upwards
    if (iteration_direction == 1) {
        while (height <= m_current_height) {
            auto it = m_utxo_map.find(height);
            if (it == m_utxo_map.end()) break; // no data here, stop or handle

            if (it->second.timestamp > timestamp) {
                return height;
            }
            height++;
        }
    }
    // Search downwards
    else {
        while (height > 0) {
            auto it = m_utxo_map.find(height);
            if (it == m_utxo_map.end()) break; // no data here, stop or handle

            if (it->second.timestamp <= timestamp) {
                return height + 1; // next height is just after timestamp
            }
            if (height == 0) break;
            height--;
        }
    }

    // If not found, return 0 or some sentinel
    return 0;
}

bool CoreAnalytics::UpdateVocdMedian()
{
    auto start_height = GetHeightAfterTimestamp(m_current_timestamp - 2592000);
    auto prev_start_height = GetHeightAfterTimestamp(m_utxo_map[std::max((int)m_current_height - 1,0)].timestamp - 2592000);
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
    m_temp_vars.mvocd_running.insert(m_row.vocd)
    m_row.mvocd = m_temp_vars.mvocd_running.median();
    return false;
}

// TODO: Create loadbtcprices function to retrieve prices from db. Perhaps this function should retrieve other things too like count and total outputs etc
std::unordered_map<int64_t, double> CoreAnalytics::LoadBTCPrices(const std::string& file_path){
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

std::optional<double> CoreAnalytics::GetBTCPrice(const std::unordered_map<int64_t, double>& btc_price_map, 
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

std::unordered_map<int64_t, double> CoreAnalytics::ConvertTableToMap(const std::shared_ptr<arrow::Table>& table) {
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
//TODO: Create functions from pseudocode
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
