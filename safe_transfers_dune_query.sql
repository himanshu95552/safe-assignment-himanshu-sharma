
-- dune query id: 4349530

WITH safe_transfers AS -- Extracts raw transfer data for Safes from the Ethereum Mainnet.
(   SELECT 
     transfers.blockchain  
    ,transfers.to  
    ,transfers."from" 
    ,transfers.block_date
    ,transfers.block_time
    ,transfers.tx_hash
    ,transfers.symbol
    ,transfers.contract_address
    ,transfers.token_standard 
    ,transfers.amount
    ,transfers.amount_usd
    FROM tokens_ethereum.transfers transfers
    WHERE 1 = 1
    AND transfers.block_date >= DATE('2023-01-01') 
    AND transfers."from" IN (SELECT safes.address from safe_ethereum.safes safes) 
    AND transfers.amount_usd > 0
    -- AND transfers.block_number > {{min_block}} - 12 -- For incremental load.
     )
, receiver_labels AS -- Adds metadata to receiver address (labels, categories, contract_names)
(   SELECT 
     transfers.blockchain
    ,transfers.to AS address
    ,MIN(la.name) AS label_names
    ,MIN(la.category) AS label_categories
    ,MIN(la.model_name) AS label_models
    ,MIN(lc.name) AS contract_name
    ,MIN(ens.name) AS ens
    FROM safe_transfers transfers
    LEFT JOIN ens.resolver_latest ens ON transfers.to = ens.address                                     -- Adds Ethereum Name Service for receivers
    LEFT JOIN labels.contracts lc ON transfers.to = lc.address AND transfers.blockchain = lc.blockchain -- Adds Conract Name for receivers
    LEFT JOIN labels.addresses la                                                                       -- Adds Label Names, Categories & Models for receivers.
        ON transfers.to = la.address 
        AND transfers.blockchain = la.blockchain 
        AND la.category NOT IN ('contracts')                                                            -- Only useful to get the contract name, which we are getting from the join above
        AND la.category NOT IN ('social')                                                               -- Just flags if the contract has ens name, not helpful
        AND la.model_name NOT IN ('validators_ethereum')
        AND (
            la.label_type NOT IN ('persona', 'usage')
            OR la.model_name IN ('dex_pools', 'dao_framework', 'mev', 'flashbots')
            )
    GROUP BY 1, 2
     )
, labelled_transfers AS 
(   SELECT
    transfers.blockchain
    ,transfers.block_date
    ,transfers.block_time
    ,transfers.tx_hash
    ,CASE 
        WHEN stb.stablecoin_address IS NOT NULL THEN 'stablecoin' 
        ELSE transfers.token_standard 
        END AS token_category
    ,transfers."from" AS safe_sender
    ,transfers.to AS receiver
    ,CASE 
        WHEN transfers.to IN (SELECT safes.address FROM safe_ethereum.safes safes) THEN 'Safe'
        WHEN transfers.to IN (SELECT addresses.address FROM labels.burn_addresses addresses) THEN 'Burner address'
        WHEN transfers.to IN (SELECT traces.address FROM ethereum.creation_traces traces) THEN 'Smart contract'
        ELSE 'EOA' 
        END AS receiver_type
    ,CASE 
        WHEN label.contract_name LIKE '%Gnosis_safe%' THEN 'Safe' 
        ELSE label.contract_name 
        END AS receiver_contract_name
    ,label.label_names AS receiver_label_names
    ,label.label_categories AS receiver_label_categories
    ,label.label_models AS receiver_label_models
    ,label.ens AS receiver_ens
    ,CASE
        WHEN label.label_models = 'dao_multisig' THEN CONCAT('Safe: ', SPLIT_PART(label.label_names, ':', 1)) --if it's a DAO safe, get the name from labels (because contract name always = GnosisSafe)
        ELSE COALESCE(SPLIT_PART(label.contract_name, ': ', 1), label.label_names, label.ens)
        END AS receiver_name -- get name from contract name if available, otherwise from labels
    ,transfers.symbol
    ,transfers.contract_address AS token_address
    ,transfers.amount
    ,transfers.amount_usd
    FROM safe_transfers transfers
    LEFT JOIN dune.safe.dataset_stablecoin_addresses_by_chain stb ON transfers.blockchain = stb.blockchain AND transfers.contract_address = stb.stablecoin_address
    LEFT JOIN receiver_labels label ON transfers.to = label.address
    WHERE transfers.amount_usd IS NOT NULL
    )
, labelled_transfers_with_vertical AS 
(   SELECT
     transfers.*
    ,CASE
        WHEN transfers.receiver_label_models = 'burn_addresses' THEN 'burn address'
        WHEN transfers.receiver_contract_name = 'Myname: WETH9' THEN 'eth wrapping'
        WHEN transfers.receiver_label_models = 'cex_ethereum' THEN 'CEX'
        WHEN transfers.receiver_label_categories = 'bridge' OR transfers.receiver_name IN ('Across_v2','Hop_protocol','Stargate','Stargate_v2','Zklink') THEN 'bridge'
        WHEN 
            transfers.receiver_label_categories = 'dex' 
            OR transfers.receiver_name IN ('Balancer_v2','Curvefi','Curve','Gnosis_protocol_v2','Lifi','Oneinch','Sushi')
            OR (transfers.receiver_type = 'Smart contract' AND LOWER(transfers.receiver_name) LIKE '%swap%') --includes: paraswap, swapr, uniswap, shibaswap,defiswap and more
                THEN 'DEX'
        WHEN transfers.receiver_name IN (
            'Aave', 'Aave_v2', 'Aave_v3','Clearpool_finance','Clearpool','Compound_v2', 'Compound_v3','Curve_lend','Echelon','Euler','Fluid','Fluxfinance','Fraxfinance',
            'Maplefinance_v2','Midas','Morpho','Morpho_blue','Morpho_aave_v2','Morpho_compound','Silo','Spark_protocol','Uwulend','Yearn'
                ) THEN 'lending'
        WHEN transfers.receiver_name IN (
            'Etherfi','Etherfiliquiditypool','Frax','Lido','Mantle','Meveth','Pirex','Rocketpool','Rockx_liquid_staking','Stader','Stakewise','Stakewise_v3','Swell_v3'
                ) THEN 'liquid staking'
        WHEN transfers.receiver_name IN (
            'Eigenlayer','Symbiotic'
                ) THEN 'restaking'
        WHEN transfers.receiver_name IN ('Kelpdao','Mellow_lrt','Renzo') THEN 'liquid restaking' --https://defillama.com/protocols/Liquid%20Restaking
        WHEN transfers.receiver_type = 'Smart contract' AND transfers.receiver IN (SELECT stakers.address FROM labels.eth_stakers stakers) THEN 'ETH staking' --captures the likes of Kiln, abyss, Stakefish
        WHEN transfers.receiver_name IN (
            'Aura_finance','Apecoin','Convex','Ethena_labs','Eth_fox_vault','Instadapp_lite','Origin_protocol','Pendle','Redacted','Tokemak','Usual'
                ) THEN 'yield'
        WHEN transfers.receiver_name IN ('Abracadabra','Amp','Lybra_finance','Maker','Threshold_network') THEN 'CDP'
        WHEN transfers.receiver_name IN ('Contango_v2', 'Dydx') THEN 'derivatives'
        WHEN transfers.receiver_name IN ('Anzen_finance_v2') THEN 'rwa' --https://defillama.com/protocols/RWA
        WHEN transfers.receiver_name IN ('Zircuit_staking') THEN 'farm' --https://defillama.com/protocols/farm
        WHEN transfers.receiver_name IN ('Llamapay') THEN 'payments'
        ELSE CONCAT('unclassified transfer to ', transfers.receiver_type) 
        END AS vertical
    FROM labelled_transfers transfers
    )
, labelled_transfers_with_vertical_and_protocol AS 
(   SELECT
     transfers.*
    ,COALESCE(transfers.receiver_contract_name, CONCAT('unclassified transfer to ', transfers.receiver_type)) AS protocol
    FROM labelled_transfers_with_vertical transfers
    )

SELECT transfers.* FROM labelled_transfers_with_vertical_and_protocol transfers
