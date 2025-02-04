# Dune queries maintained by the TON Foundation

# Materialized views

## [dune.ton_foundation.result_jetton_price_daily](./queries/jetton_price_daily___4438952.sql)

Aggregates all jettons prices nominated in TON and USD for all kind of assets:
* Storm SLP - price is determined based on assetsdeposits into SLP pools
* DEX LPs - price is determined based on [ton.dex_pools](https://docs.dune.com/data-catalog/ton/dex_pools) table
* other jettons traded on DEXs - price is determined based on [ton.dex_trades](https://docs.dune.com/data-catalog/ton/dex_trades) table
