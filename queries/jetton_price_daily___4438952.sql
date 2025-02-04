-- part of a query repo
-- query name: jetton_price_daily
-- query link: https://dune.com/queries/4438952


------------------------------------------
------------------------------------------
------------------------------------------
------------------------------------------
-- brave approach of creating a price feed
-- of all tokens on TON Blockchain -------
-- including: TON, Jettons, LP, SLP ------
------------------------------------------
-- made by @okhlopkov @pshuvalov @ TF ----
------------------------------------------
------------------------------------------
------------------------------------------
------------------------------------------

------------------------------------------
--------------DEX TRADES------------------
------------------------------------------


WITH
DT AS (
    SELECT 
        CASE
            WHEN token_sold_address = UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe') 
                THEN token_bought_address
            WHEN token_bought_address != UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe')
                 AND token_sold_address IN (
                     '0:0000000000000000000000000000000000000000000000000000000000000000',
                     UPPER('0:8cdc1d7640ad5ee326527fc1ad0514f468b30dc84b0173f0e155f451b4e11f7c')
                 )
                THEN token_bought_address
            ELSE token_sold_address
        END AS token_address,
        CASE
            WHEN token_sold_address = UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe') 
                THEN amount_bought_raw
            WHEN token_bought_address != UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe')
                 AND token_sold_address IN ( 
                     '0:0000000000000000000000000000000000000000000000000000000000000000',
                     UPPER('0:8cdc1d7640ad5ee326527fc1ad0514f468b30dc84b0173f0e155f451b4e11f7c')
                 )
                THEN amount_bought_raw
            ELSE amount_sold_raw
        END AS amount_raw,
        volume_usd,
        volume_ton,
        block_time,
        trader_address
    FROM ton.dex_trades
    WHERE amount_bought_raw > 100 AND amount_sold_raw > 100
)

-- add missing USDT trades to store all prices in one table
, USDT_TRADES AS (
    SELECT 
        UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe') AS token_address,
        CAST(amount_raw AS DOUBLE) / volume_ton * volume_usd * POWER(10, 6-9) AS amount_raw, -- ton
        volume_usd, 
        volume_ton,
        block_time,
        trader_address
    FROM DT
    WHERE token_address = '0:0000000000000000000000000000000000000000000000000000000000000000'
)

, ALL_TRADES AS (
    SELECT *
    FROM DT
    UNION ALL
    SELECT * 
    FROM USDT_TRADES
)

, PRICES_FROM_DEX_TRADES AS (
    SELECT
        token_address,
        DATE_TRUNC('day', block_time) AS ts,
        SUM(volume_ton) / SUM(CAST(amount_raw AS DOUBLE)) AS price_ton,
        SUM(volume_usd) / SUM(CAST(amount_raw AS DOUBLE)) AS price_usd
    FROM ALL_TRADES
    GROUP BY 1, 2
    HAVING 1=1
        AND COUNT(*) >= 100  -- 100 trades / day
        AND COUNT(DISTINCT trader_address) >= 10 -- 10 traders / day
        
)

------------------------------------------
--------------LP TOKENS-------------------
------------------------------------------

, PRICES_LP_TOKENS AS (
    SELECT 
        pool AS token_address, 
        block_date AS ts,
        MAX_BY(CAST(tvl_ton AS DOUBLE) / total_supply, block_time) AS price_ton,
        MAX_BY(CAST(tvl_usd AS DOUBLE) / total_supply, block_time) AS price_usd
    FROM ton.dex_pools
    WHERE 1=1
        AND total_supply > 0
        AND tvl_usd > 1000  -- $1000 TVL 
    GROUP BY 1,2
    HAVING 1=1
        -- AND COUNT(*) >= 2 -- 2 LP CHANGES/day, maybe too much
)


------------------------------------------
--------------SLP TOKENS------------------
------------------------------------------


, SLPs as ( -- list of slp jettons and underlying assets
  SELECT 'NOT-SLP' as asset,
  UPPER('0:2ab634cfcbdbe3b97503691e0780c3d07c9069210a2b24b991ba4f9941b453f9') as slp_address,
  UPPER('0:2f956143c461769579baef2e32cc2d7bc18283f40d20bb03e432cd603ac33ffc') as underlying_asset

  UNION ALL

  SELECT 'USDT-SLP' as asset,
  UPPER('0:aea78c710ae94270dc263a870cf47b4360f53cc5ed38e3db502e9e9afb904b11') as slp_address,
  UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe') as underlying_asset

  UNION ALL

  SELECT 'TON-SLP' as asset,
  UPPER('0:8d636010dd90d8c0902ac7f9f397d8bd5e177f131ee2cca24ce894f15d19ceea') as slp_address,
  UPPER('0:0000000000000000000000000000000000000000000000000000000000000000') as underlying_asset
  
)

, SLP_MINTS AS ( -- get all mints event
  SELECT trace_id, je.amount AS slp_amount, block_date, SLPs.* 
  FROM ton.jetton_events je
  JOIN SLPs 
      ON je.jetton_master = SLPs.slp_address
  WHERE 1=1
      AND type = 'mint'
      AND NOT tx_aborted
)

, SLP_DEPOSITS AS ( -- each mint has deposit

    -- jettons assets
    SELECT SLP_MINTS.*, je.amount as underlying_asset_amount 
    FROM ton.jetton_events je
    JOIN SLP_MINTS ON 1=1
        AND SLP_MINTS.block_date = je.block_date 
        AND je.trace_id = SLP_MINTS.trace_id
        AND je.jetton_master = SLP_MINTS.underlying_asset 
        AND NOT tx_aborted 
        AND SLP_MINTS.underlying_asset != '0:0000000000000000000000000000000000000000000000000000000000000000'
    
    -- special version for native TON 
    -- let's take the first message (contains TON to be deposited + ~0.4 for gas fees) 
    -- minus the last one (contains excesses)

    UNION ALL
    
    SELECT 
        SLP_MINTS.*,
        MIN_BY(value, created_lt) - MAX_BY(value, created_lt) AS underlying_asset_amount 
    FROM ton.messages M
    JOIN SLP_MINTS ON 1=1
        AND SLP_MINTS.block_date = M.block_date 
        AND M.trace_id = SLP_MINTS.trace_id
        AND SLP_MINTS.underlying_asset = '0:0000000000000000000000000000000000000000000000000000000000000000'
    GROUP BY 1, 2, 3, 4, 5, 6

)

, SLP_ADDRESS_INTER_PRICE AS (
    SELECT 
        block_date, asset, slp_address, underlying_asset, 
        CAST(1.0 AS DOUBLE) * SUM(underlying_asset_amount) / SUM(slp_amount) AS price
    FROM SLP_DEPOSITS
    GROUP BY 1, 2, 3, 4
)

, PRICES_SLP AS (
    SELECT 
        slp_address AS token_address,
        block_date AS ts,
        SLP_ADDRESS_INTER_PRICE.price * P.price_ton AS price_ton,
        SLP_ADDRESS_INTER_PRICE.price * P.price_usd AS price_usd
    FROM SLP_ADDRESS_INTER_PRICE
    INNER JOIN PRICES_FROM_DEX_TRADES P
        ON P.token_address = SLP_ADDRESS_INTER_PRICE.underlying_asset
        AND P.ts = SLP_ADDRESS_INTER_PRICE.block_date
)

------------------------------------------
-----------FINAL GRAND MERGE -------------
------------------------------------------

------- as a time went by we realised that 
------- these column names are not ideal -
------- but we should keep them ----------
------- otherwise all queries will break -
------- that is not what we want ---------

SELECT 
    COALESCE(
        PRICES_SLP.token_address,
        PRICES_LP_TOKENS.token_address,
        PRICES_FROM_DEX_TRADES.token_address
    ) AS token_address,
    
    COALESCE(
        PRICES_SLP.ts,
        PRICES_LP_TOKENS.ts,
        PRICES_FROM_DEX_TRADES.ts
    ) AS ts,

    COALESCE(
        PRICES_SLP.price_ton,
        PRICES_LP_TOKENS.price_ton,
        PRICES_FROM_DEX_TRADES.price_ton
    ) AS price_ton,

    COALESCE(
        PRICES_SLP.price_usd,
        PRICES_LP_TOKENS.price_usd,
        PRICES_FROM_DEX_TRADES.price_usd
    ) AS price_usd
    
FROM PRICES_FROM_DEX_TRADES
FULL OUTER JOIN PRICES_SLP
    ON PRICES_FROM_DEX_TRADES.token_address = PRICES_SLP.token_address
    AND PRICES_FROM_DEX_TRADES.ts = PRICES_SLP.ts
FULL OUTER JOIN PRICES_LP_TOKENS
    ON PRICES_FROM_DEX_TRADES.token_address = PRICES_LP_TOKENS.token_address
    AND PRICES_FROM_DEX_TRADES.ts = PRICES_LP_TOKENS.ts
