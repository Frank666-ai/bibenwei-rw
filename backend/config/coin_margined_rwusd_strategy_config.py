"""Configuration for the independent coin-margined RWUSD hedge strategy."""


class CoinMarginedRWUSDStrategyConfig:
    NAME = "Coin-margined RWUSD yield hedge strategy"
    SOURCE = "DBRSI/coin-margined-rwusd-independent"
    MODE = "coin_margined_direct_pm_collateral"
    MAINNET_MODE = "binance_portfolio_margin_live"
    TESTNET_MODE = "binance_futures_demo"
    MAINNET_EXECUTION_ENABLED = True
    TESTNET_EXECUTION_ENABLED = False

    ALLOCATION_SYMBOLS = (
        "BTCUSDC",
        "ETHUSDC",
    )
    CANDIDATE_SYMBOLS = ALLOCATION_SYMBOLS
    SYMBOL_TARGET_LEVERAGE = {
        "BTCUSDC": 50,
        "ETHUSDC": 50,
    }
    KLINE_INTERVAL = "30m"
    KLINE_LIMIT = 300
    ALLOCATION_KLINE_INTERVAL = "5m"
    ALLOCATION_KLINE_LIMIT = 300
    POLL_INTERVAL_SEC = 5.0
    CAPITAL_SCAN_INTERVAL_SEC = 30.0
    ALLOCATION_RESERVE_REEVALUATE_SEC = 15 * 60.0
    DEPOSIT_SCAN_OVERLAP_SEC = 10 * 60.0
    RWUSD_ACCOUNT_REFRESH_SEC = 5 * 60.0
    RWUSD_RATE_REFRESH_SEC = 60 * 60.0
    SELECTOR_INPUT_REFRESH_SEC = 60.0
    SELECTOR_INPUT_MAX_STALE_SEC = 15 * 60.0

    TARGET_NOTIONAL = 1000.0
    TOTAL_MARGIN_FRACTION = 0.80
    PER_SIDE_MARGIN_FRACTION = 0.40
    SIM_LEVERAGE = 50.0
    DRY_RUN_INITIAL_CAPITAL = 10_000.0
    OPENING_TRANCHE_FRACTION = 0.05
    HARVEST_TRANCHE_FRACTION = 0.05
    TARGET_REBALANCE_BAND_FRACTION = 0.05
    OPENING_RETRY_COOLDOWN_CYCLES = 12
    OPENING_MAX_PAIR_FAILURES = 3
    REPAIR_RETRY_COOLDOWN_CYCLES = 3
    EXCHANGE_MINIMUM_RETRY_COOLDOWN_CYCLES = 5
    EXCHANGE_MINIMUM_UNKNOWN_RETRY_GROWTH_FRACTION = 0.25

    SELECTOR_EVAL_INTERVAL_MINUTES = 15
    SELECTOR_SWITCH_EDGE = 0.20
    SELECTOR_SWITCH_COOLDOWN_MINUTES = 60
    MIN_SYMBOL_CAPACITY_RATIO = 0.90
    RISK_PROFILE_REFRESH_SEC = 15 * 60.0
    LEVERAGE_STATUS_REFRESH_SEC = 15 * 60.0
    LEVERAGE_UPGRADE_RETRY_SEC = 60 * 60.0
    ALLOCATION_MIN_DAILY_RETURN = 0.10
    ALLOCATION_MAX_SPREAD_BPS = 5.0
    ALLOCATION_ENTRY_DEPTH_BPS = 10.0
    ALLOCATION_ENTRY_DEPTH_UTILIZATION = 0.50
    ALLOCATION_OFFICIAL_CAPACITY_HAIRCUT = 0.75
    ALLOCATION_LIQUIDITY_RESERVE = 0.05
    ALLOCATION_APPRECIATION_EXEMPT_SYMBOLS = (
        "BTCUSDC",
        "ETHUSDC",
    )
    OPENING_VOLATILITY_MAX_RANGE = 0.015
    OPENING_VOLATILITY_ATR_MULTIPLIER = 1.50
    OPENING_VOLATILITY_ATR_PERIOD = 4
    EXIT_VOLATILITY_MAX_RANGE = 0.010
    EXIT_VOLATILITY_ATR_MULTIPLIER = 1.25
    EXIT_VOLATILITY_ATR_PERIOD = 14
    ALLOCATION_TARGET_WEIGHTS = {
        "BTCUSDC": 0.38,
        "ETHUSDC": 0.42,
    }
    ALLOCATION_CORE_SYMBOLS = ("BTCUSDC", "ETHUSDC")
    ALLOCATION_MIN_CORE_FRACTION = 0.70
    ALLOCATION_MAX_SYMBOL_FRACTION = 0.40
    ALLOCATION_MAX_SYMBOL_FRACTIONS = {
        "BTCUSDC": 0.38,
        "ETHUSDC": 0.42,
    }
    ALLOCATION_MAX_CORRELATION_CLUSTER_FRACTION = 0.80
    ALLOCATION_CORRELATION_THRESHOLD = 0.80

    SOFT_UNIMMR = 4.5
    HARD_UNIMMR = 4.0
    HARD_RISK_TARGET_UNIMMR = 5.0
    HARD_RISK_RELEASE_UNIMMR = 5.5
    HARD_RISK_RELEASE_STABLE_SEC = 30 * 60.0
    HARD_RISK_MIN_REDUCTION_FRACTION = 0.10
    HARD_RISK_MAX_REDUCTION_FRACTION = 0.25
    HARD_RISK_MAX_SLIPPAGE_BPS = 5.0
    HARD_RISK_DEPTH_UTILIZATION = 0.10
    HARD_RISK_MAX_BATCH_NOTIONAL_PER_SIDE = 100_000.0
    PROJECTED_UNIMMR_BUFFER = 0.10
    MAX_DRAWDOWN = 0.15
    REDEEM_UNIMMR = 0.0
    MAX_TOTAL_NET_LEVERAGE = 1.25
    MAX_SINGLE_SYMBOL_NET_LEVERAGE = 1.25
    HARD_AVAILABLE_BALANCE = 0.0

    EXIT_POLL_INTERVAL_SEC = 2.0
    EXIT_BATCH_FRACTION = 0.10
    EXIT_MAX_SLIPPAGE_BPS = 8.0
    EXIT_DEPTH_UTILIZATION = 0.20
    EXIT_MAX_BATCH_NOTIONAL_PER_SIDE = 250_000.0
    EXIT_LIMIT_REPRICE_SEC = 15.0
    EXIT_RETRY_BASE_SEC = 2.0
    EXIT_RETRY_MAX_SEC = 60.0
    EXIT_REDEMPTION_HISTORY_POLL_SEC = 15.0
    EXIT_REDEMPTION_QUOTA_RETRY_SEC = 5 * 60.0
    EXIT_REDEMPTION_UNCERTAIN_RETRY_SEC = 10 * 60.0
    EXIT_ZERO_CONFIRM_SEC = 30.0
    EXIT_REDEMPTION_PREFERENCE = ("STANDARD", "FAST")
    EXIT_RWUSD_DUST = 0.00000001
    EXIT_USDC_DUST = 0.000001

    HARVEST_TRIGGER_ROE = 1.50
    HARVEST_MIN_NET_USD = 1.0
    HARVEST_COST_MULTIPLIER = 3.0
    MAKER_FEE_FALLBACK_BPS = 2.0
    MAKER_EXECUTION_BUFFER_BPS = 0.0

    MIN_SWEEP = 1.0
    MIN_REDEEM = 50.0
    RWUSD_APR = 0.12
    RWUSD_SOURCE_ASSETS = ("USDC", "USDT")
    RWUSD_HARVEST_SOURCE_ASSET = "USDC"
    RWUSD_HARVEST_SUBSCRIPTION_ASSET = "USDT"
    RWUSD_TESTNET_SOURCE_ASSETS = ("USDC",)
    LIVE_HARVEST_TRANSFER_UNIMMR_FLOOR = SOFT_UNIMMR
    LIVE_HARVEST_TRANSFER_MAX_EQUITY_FRACTION = 0.05
    LIVE_HARVEST_TRANSFER_HEADROOM_FRACTION = 0.001
    LIVE_HARVEST_TRANSFER_HEADROOM_MIN_USD = 0.01
    LIVE_HARVEST_TRANSFER_RETRY_BACKOFF_SEC = (1.0, 3.0, 10.0, 30.0)
    PROFIT_SWEEP_FAST_POLL_INTERVAL_SEC = 1.0
    PROFIT_SWEEP_MAX_WAIT_SEC = 90.0

    DRY_RUN_FILL_FRACTION = 1.0
    DRY_RUN_MIN_FILL_QUANTITY = 0.0
    DRY_RUN_ORDER_TIMEOUT_CYCLES = 3
    DRY_RUN_MAX_REQUOTES = 2
    MIN_AVAILABLE_BALANCE = 100.0
    EXTERNAL_MAX_SUBMISSION_ATTEMPTS = 3
    EXTERNAL_ORDER_NOT_FOUND_CONFIRMATIONS = 3
    MAKER_ORDER_TIMEOUT_CYCLES = 3
    OPENING_CATCHUP_REPRICE_CYCLES = 1
    MAKER_MAX_REQUOTES = 12
    TESTNET_REST_BASE_URL = "https://demo-fapi.binance.com"
    MAINNET_PAPI_BASE_URL = "https://papi.binance.com"
    SPOT_MAINNET_BASE_URL = "https://api.binance.com"

    @classmethod
    def get_config_dict(cls) -> dict:
        return {
            "name": cls.NAME,
            "source": cls.SOURCE,
            "mode": cls.MODE,
            "collateral": {
                "source": "Portfolio Margin BTC or ETH",
                "single_asset_only": True,
                "symbols": list(cls.CANDIDATE_SYMBOLS),
                "profit_loop": (
                    "PM profit -> Spot confirmation -> restore hedge; "
                    "RWUSD subscription -> PM collateral asynchronously"
                ),
                "ordinary_position_reduction_enabled": False,
            },
            "execution_enabled": (
                cls.TESTNET_EXECUTION_ENABLED
                or cls.MAINNET_EXECUTION_ENABLED
            ),
            "execution_modes": {
                "mainnet": {
                    "mode": cls.MAINNET_MODE,
                    "execution_enabled": cls.MAINNET_EXECUTION_ENABLED,
                    "orders": "portfolio_margin_usdc_limit_gtx",
                    "portfolio_margin_supported": True,
                    "official_uni_mmr_supported": True,
                    "rwusd_subscription": "real_mainnet_sapi",
                    "initial_collateral": (
                        "single_btc_or_eth_direct_portfolio_margin_collateral"
                    ),
                    "rwusd_source_assets": list(cls.RWUSD_SOURCE_ASSETS),
                    "account_info_source": (
                        "sapi_v1_v2_portfolio_account_with_papi_fallback"
                    ),
                    "collateral_balance_source": (
                        "sapi_v1_portfolio_balance_with_papi_fallback"
                    ),
                    "api_key_permissions_required": [
                        "enableReading",
                        "enablePortfolioMarginTrading",
                        "enableSpotAndMarginTrading",
                        "permitsUniversalTransfer",
                        "ipRestrict",
                    ],
                    "api_key_withdrawals_required": False,
                    "api_trading_lock_required": False,
                    "rest_base_url": cls.MAINNET_PAPI_BASE_URL,
                },
                "testnet": {
                    "mode": "unsupported",
                    "execution_enabled": cls.TESTNET_EXECUTION_ENABLED,
                    "orders": "disabled",
                    "rwusd_transfers": "disabled",
                    "initial_collateral": "unsupported",
                    "rwusd_source_assets": list(
                        cls.RWUSD_TESTNET_SOURCE_ASSETS
                    ),
                    "rwusd_testnet_supported": False,
                    "portfolio_margin_supported": False,
                    "official_uni_mmr_supported": False,
                    "rest_base_url": cls.TESTNET_REST_BASE_URL,
                },
            },
            "universe": {
                "candidate_symbols": list(cls.CANDIDATE_SYMBOLS),
                "kline_interval": cls.KLINE_INTERVAL,
                "kline_limit": cls.KLINE_LIMIT,
                "allocation_kline_interval": cls.ALLOCATION_KLINE_INTERVAL,
                "allocation_kline_limit": cls.ALLOCATION_KLINE_LIMIT,
                "poll_interval_sec": cls.POLL_INTERVAL_SEC,
                "capital_scan_interval_sec": cls.CAPITAL_SCAN_INTERVAL_SEC,
                "rwusd_account_refresh_sec": (
                    cls.RWUSD_ACCOUNT_REFRESH_SEC
                ),
                "rwusd_rate_refresh_sec": cls.RWUSD_RATE_REFRESH_SEC,
                "selector_input_refresh_sec": (
                    cls.SELECTOR_INPUT_REFRESH_SEC
                ),
                "selector_input_max_stale_sec": (
                    cls.SELECTOR_INPUT_MAX_STALE_SEC
                ),
            },
            "positioning": {
                "sizing_mode": "single_collateral_symbol_deployed_capital",
                "dry_run_equity_source": "initial_capital",
                "total_margin_fraction": cls.TOTAL_MARGIN_FRACTION,
                "per_side_margin_fraction": cls.PER_SIDE_MARGIN_FRACTION,
                "leverage": cls.SIM_LEVERAGE,
                "leverage_semantics": (
                    "account target with exchange-confirmed runtime sizing"
                ),
                "leverage_status_refresh_sec": (
                    cls.LEVERAGE_STATUS_REFRESH_SEC
                ),
                "leverage_upgrade_retry_sec": (
                    cls.LEVERAGE_UPGRADE_RETRY_SEC
                ),
                "opening_tranche_fraction": cls.OPENING_TRANCHE_FRACTION,
                "harvest_tranche_fraction": (
                    cls.HARVEST_TRANCHE_FRACTION
                ),
                "target_rebalance_band_fraction": (
                    cls.TARGET_REBALANCE_BAND_FRACTION
                ),
                "target_formula": (
                    "lane_deployed_capital * total_margin_fraction "
                    "* exchange_confirmed_leverage / 2"
                ),
                "target_decrease_action": "hold_existing_hedge",
                "projected_unimmr_floor": (
                    cls.SOFT_UNIMMR + cls.PROJECTED_UNIMMR_BUFFER
                ),
            },
            "allocation": {
                "symbols": list(cls.ALLOCATION_SYMBOLS),
                "mode": "fixed_single_collateral_symbol",
                "maximum_active_lanes": 1,
                "initial_collateral_allocation_fraction": 1.0,
                "liquidity_reserve_fraction": 0.0,
                "incremental_external_capital_enabled": False,
                "profit_reinvestment_keeps_origin_lane": True,
            },
            "volatility": {
                "opening_max_range": cls.OPENING_VOLATILITY_MAX_RANGE,
                "opening_atr_multiplier": (
                    cls.OPENING_VOLATILITY_ATR_MULTIPLIER
                ),
                "exit_max_range": cls.EXIT_VOLATILITY_MAX_RANGE,
                "exit_atr_multiplier": cls.EXIT_VOLATILITY_ATR_MULTIPLIER,
                "opening_atr_period": cls.OPENING_VOLATILITY_ATR_PERIOD,
                "exit_atr_period": cls.EXIT_VOLATILITY_ATR_PERIOD,
                "basis": (
                    "completed_30m_range_with_wilder_atr4_for_open_atr14_for_exit"
                ),
            },
            "selector": {
                "mode": "fixed_to_collateral_symbol",
                "switch_enabled": False,
                "eval_interval_minutes": cls.SELECTOR_EVAL_INTERVAL_MINUTES,
                "switch_edge": cls.SELECTOR_SWITCH_EDGE,
                "switch_cooldown_minutes": cls.SELECTOR_SWITCH_COOLDOWN_MINUTES,
                "minimum_symbol_capacity_ratio": (
                    cls.MIN_SYMBOL_CAPACITY_RATIO
                ),
                "risk_profile_refresh_sec": (
                    cls.RISK_PROFILE_REFRESH_SEC
                ),
            },
            "risk": {
                "soft_unimmr": cls.SOFT_UNIMMR,
                "hard_unimmr": cls.HARD_UNIMMR,
                "hard_risk_target_unimmr": cls.HARD_RISK_TARGET_UNIMMR,
                "hard_risk_release_unimmr": cls.HARD_RISK_RELEASE_UNIMMR,
                "hard_risk_release_stable_sec": (
                    cls.HARD_RISK_RELEASE_STABLE_SEC
                ),
                "hard_risk_min_reduction_fraction": (
                    cls.HARD_RISK_MIN_REDUCTION_FRACTION
                ),
                "hard_risk_max_reduction_fraction": (
                    cls.HARD_RISK_MAX_REDUCTION_FRACTION
                ),
                "max_drawdown": cls.MAX_DRAWDOWN,
                "redeem_unimmr": cls.REDEEM_UNIMMR,
                "max_total_net_leverage": cls.MAX_TOTAL_NET_LEVERAGE,
                "max_single_symbol_net_leverage": cls.MAX_SINGLE_SYMBOL_NET_LEVERAGE,
                "hard_available_balance": cls.HARD_AVAILABLE_BALANCE,
                "gross_exposure_guard": (
                    "exchange_notional_limit_and_uni_mmr"
                ),
                "soft_risk_action": (
                    "freeze_expansion_and_new_subscription_continue_harvest"
                ),
                "hard_risk_action": (
                    "freeze_expansion_and_depth_limited_gross_reduction"
                ),
                "hedge_gap_action": (
                    "normal_add_missing_side_only"
                ),
                "automatic_loss_side_reduction": (
                    "hard_risk_only_uni_mmr_available_margin_liquidation"
                ),
            },
            "harvest": {
                "trigger_roe": cls.HARVEST_TRIGGER_ROE,
                "tranche_fraction": cls.HARVEST_TRANCHE_FRACTION,
                "minimum_net_usd": cls.HARVEST_MIN_NET_USD,
                "cost_multiplier": cls.HARVEST_COST_MULTIPLIER,
                "order_type": "LIMIT",
                "time_in_force": "GTX",
                "maker_only": True,
                "dynamic_commission_rate": True,
                "maker_fee_fallback_bps": cls.MAKER_FEE_FALLBACK_BPS,
                "execution_buffer_bps": cls.MAKER_EXECUTION_BUFFER_BPS,
            },
            "rwusd": {
                "min_sweep": cls.MIN_SWEEP,
                "min_redeem": cls.MIN_REDEEM,
                "apr": cls.RWUSD_APR,
                "source_assets": list(cls.RWUSD_SOURCE_ASSETS),
                "preferred_source_asset": cls.RWUSD_SOURCE_ASSETS[0],
                "harvest_source_asset": cls.RWUSD_HARVEST_SOURCE_ASSET,
                "harvest_subscription_asset": (
                    cls.RWUSD_HARVEST_SUBSCRIPTION_ASSET
                ),
                "mode": "mainnet_profit_reinvestment_only",
                "testnet_supported": False,
                "mainnet_subscription_enabled": cls.MAINNET_EXECUTION_ENABLED,
                "mainnet_collateral_transfer": (
                    "portfolio_earn_asset_transfer"
                ),
                "mainnet_collateral_balance": (
                    "sapi_v1_portfolio_balance_with_papi_fallback"
                ),
                "initial_collateral_required_before_orders": True,
                "rwusd_redeem_for_normal_opening": False,
                "mainnet_redemption_enabled": (
                    cls.MAINNET_EXECUTION_ENABLED
                ),
                "harvest_transfer_unimmr_floor": (
                    cls.LIVE_HARVEST_TRANSFER_UNIMMR_FLOOR
                ),
                "harvest_transfer_max_equity_fraction": (
                    cls.LIVE_HARVEST_TRANSFER_MAX_EQUITY_FRACTION
                ),
                "harvest_transfer_headroom_fraction": (
                    cls.LIVE_HARVEST_TRANSFER_HEADROOM_FRACTION
                ),
                "harvest_transfer_headroom_min_usd": (
                    cls.LIVE_HARVEST_TRANSFER_HEADROOM_MIN_USD
                ),
                "harvest_transfer_retry_backoff_sec": (
                    list(cls.LIVE_HARVEST_TRANSFER_RETRY_BACKOFF_SEC)
                ),
                "profit_sweep_fast_poll_interval_sec": (
                    cls.PROFIT_SWEEP_FAST_POLL_INTERVAL_SEC
                ),
                "profit_sweep_max_wait_sec": (
                    cls.PROFIT_SWEEP_MAX_WAIT_SEC
                ),
            },
            "exit": {
                "poll_interval_sec": cls.EXIT_POLL_INTERVAL_SEC,
                "batch_fraction": cls.EXIT_BATCH_FRACTION,
                "max_slippage_bps": cls.EXIT_MAX_SLIPPAGE_BPS,
                "depth_utilization": cls.EXIT_DEPTH_UTILIZATION,
                "max_batch_notional_per_side": (
                    cls.EXIT_MAX_BATCH_NOTIONAL_PER_SIDE
                ),
                "limit_reprice_sec": cls.EXIT_LIMIT_REPRICE_SEC,
                "normal_order_type": "LIMIT",
                "normal_time_in_force": "GTX",
                "maker_only": True,
                "subminimum_residual_order_type": "MARKET",
                "redemption_preference": list(
                    cls.EXIT_REDEMPTION_PREFERENCE
                ),
                "redemption_history_poll_sec": (
                    cls.EXIT_REDEMPTION_HISTORY_POLL_SEC
                ),
                "redemption_quota_retry_sec": (
                    cls.EXIT_REDEMPTION_QUOTA_RETRY_SEC
                ),
                "redemption_uncertain_retry_sec": (
                    cls.EXIT_REDEMPTION_UNCERTAIN_RETRY_SEC
                ),
                "zero_confirm_sec": cls.EXIT_ZERO_CONFIRM_SEC,
                "rwusd_dust": cls.EXIT_RWUSD_DUST,
            },
            "dry_run_execution": {
                "fill_fraction": cls.DRY_RUN_FILL_FRACTION,
                "min_fill_quantity": cls.DRY_RUN_MIN_FILL_QUANTITY,
                "order_timeout_cycles": cls.DRY_RUN_ORDER_TIMEOUT_CYCLES,
                "max_requotes": cls.DRY_RUN_MAX_REQUOTES,
            },
            "exchange_execution": {
                "minimum_available_balance": cls.MIN_AVAILABLE_BALANCE,
                "max_submission_attempts": cls.EXTERNAL_MAX_SUBMISSION_ATTEMPTS,
                "order_not_found_confirmations": (
                    cls.EXTERNAL_ORDER_NOT_FOUND_CONFIRMATIONS
                ),
                "hedge_mode_required": True,
                "limit_order_filters_required": True,
                "client_order_id_idempotency": True,
                "testnet_margin_health_source": (
                    "USDC.marginBalance/USDC.maintMargin"
                ),
                "mainnet_margin_health_source": "portfolio_margin_uniMMR",
            },
            "maker_execution": {
                "normal_order_type": "LIMIT",
                "normal_time_in_force": "GTX",
                "emergency_market_purposes": [
                    "manual_flatten",
                    "customer_exit_residual",
                    "hard_risk_reduction",
                ],
                "timeout_cycles": cls.MAKER_ORDER_TIMEOUT_CYCLES,
                "opening_catchup_reprice_cycles": (
                    cls.OPENING_CATCHUP_REPRICE_CYCLES
                ),
                "max_requotes": cls.MAKER_MAX_REQUOTES,
                "opening_pair_price_factor": 1.00001,
                "opening_catchup_price": "opposite_touch_minus_one_tick",
                "opening_retry_cooldown_cycles": (
                    cls.OPENING_RETRY_COOLDOWN_CYCLES
                ),
                "opening_max_pair_failures": (
                    cls.OPENING_MAX_PAIR_FAILURES
                ),
                "repair_retry_cooldown_cycles": (
                    cls.REPAIR_RETRY_COOLDOWN_CYCLES
                ),
                "exchange_minimum_retry_cooldown_cycles": (
                    cls.EXCHANGE_MINIMUM_RETRY_COOLDOWN_CYCLES
                ),
                "exchange_minimum_unknown_retry_growth_fraction": (
                    cls.EXCHANGE_MINIMUM_UNKNOWN_RETRY_GROWTH_FRACTION
                ),
            },
        }
