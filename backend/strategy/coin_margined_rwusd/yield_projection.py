"""Auditable RWUSD yield and collateral-capacity projections."""

from __future__ import annotations

import math


def build_rwusd_yield_projection(
    *,
    profit_bucket: dict,
    leverage_control: dict,
    margin_fraction: float,
    target_leverage: float,
    mainnet_execution: bool,
    fallback_capital: float,
    shadow_apr: float,
) -> dict:
    official_balance = max(
        0.0,
        float(profit_bucket.get("exchange_rwusd_amount", 0.0) or 0.0),
    )
    if official_balance > 0.0:
        rwusd_balance = official_balance
        official_rate_time_ms = max(
            0,
            int(
                profit_bucket.get(
                    "exchange_rwusd_rate_time_ms",
                    0,
                )
                or 0
            ),
        )
        apr_value = float(
            profit_bucket.get(
                "exchange_rwusd_annual_percentage_rate",
                0.0,
            )
            or 0.0
        )
        apr = (
            apr_value
            if (
                math.isfinite(apr_value)
                and apr_value >= 0.0
                and (official_rate_time_ms > 0 or apr_value > 0.0)
            )
            else None
        )
        balance_source = "binance_official_rwusd_amount"
    else:
        shadow_principal = max(
            0.0,
            float(profit_bucket.get("rwusd_principal", 0.0) or 0.0),
        )
        shadow_interest = max(
            0.0,
            float(profit_bucket.get("rwusd_interest_accrued", 0.0) or 0.0),
        )
        rwusd_balance = shadow_principal + shadow_interest
        apr = (
            max(0.0, float(shadow_apr))
            if not mainnet_execution and rwusd_balance > 0.0
            else None
        )
        balance_source = "rwusd_shadow_ledger"

    capital_basis = max(
        0.0,
        float(
            profit_bucket.get(
                "capital_contribution_total",
                fallback_capital,
            )
            or 0.0
        ),
    )
    current_leverage = max(
        0.0,
        float(
            leverage_control.get(
                "sizing_leverage",
                target_leverage,
            )
            or 0.0
        ),
    )
    target_leverage_value = max(0.0, float(target_leverage or 0.0))
    projection = {
        "available": False,
        "basis": "daily_compounded_rwusd_apr",
        "annualization_days": 365.0,
        "rwusd_balance": rwusd_balance,
        "rwusd_apr": apr,
        "rwusd_effective_apy": None,
        "projected_1d_income": None,
        "projected_365d_income": None,
        "projected_rwusd_balance": None,
        "capital_basis": capital_basis,
        "projected_365d_return_on_capital": None,
        "margin_fraction": max(0.0, float(margin_fraction or 0.0)),
        "current_sizing_leverage": current_leverage,
        "target_leverage": target_leverage_value,
        "current_rwusd_notional_capacity": None,
        "target_rwusd_notional_capacity": None,
        "projected_current_notional_capacity": None,
        "projected_target_notional_capacity": None,
        "interest_created_current_notional_capacity": None,
        "interest_created_target_notional_capacity": None,
        "balance_source": balance_source,
        "trading_return_included": False,
        "capacity_is_not_profit": True,
        "assumptions": [
            "APR remains unchanged for the next 365 days",
            "RWUSD yield remains in the collateral loop and compounds daily",
            "future trading PnL, fees, funding and execution slippage are excluded",
            "notional capacity is not guaranteed trading profit",
        ],
        "unavailable_reason": None,
    }
    if rwusd_balance <= 0.0:
        projection["unavailable_reason"] = "rwusd_balance_unavailable"
        return projection
    if apr is None:
        projection["unavailable_reason"] = (
            "official_rwusd_apr_unavailable"
            if mainnet_execution
            else "rwusd_apr_unavailable"
        )
        return projection

    daily_rate = apr / 365.0
    effective_apy = math.pow(1.0 + daily_rate, 365.0) - 1.0
    projected_income = rwusd_balance * effective_apy
    projected_balance = rwusd_balance + projected_income
    projection.update(
        {
            "available": all(
                math.isfinite(value)
                for value in (
                    effective_apy,
                    projected_income,
                    projected_balance,
                )
            ),
            "rwusd_effective_apy": effective_apy,
            "projected_1d_income": rwusd_balance * daily_rate,
            "projected_365d_income": projected_income,
            "projected_rwusd_balance": projected_balance,
            "projected_365d_return_on_capital": (
                projected_income / capital_basis
                if capital_basis > 0.0
                else None
            ),
            "current_rwusd_notional_capacity": (
                rwusd_balance
                * projection["margin_fraction"]
                * current_leverage
            ),
            "target_rwusd_notional_capacity": (
                rwusd_balance
                * projection["margin_fraction"]
                * target_leverage_value
            ),
            "projected_current_notional_capacity": (
                projected_balance
                * projection["margin_fraction"]
                * current_leverage
            ),
            "projected_target_notional_capacity": (
                projected_balance
                * projection["margin_fraction"]
                * target_leverage_value
            ),
            "interest_created_current_notional_capacity": (
                projected_income
                * projection["margin_fraction"]
                * current_leverage
            ),
            "interest_created_target_notional_capacity": (
                projected_income
                * projection["margin_fraction"]
                * target_leverage_value
            ),
        }
    )
    if not projection["available"]:
        projection["unavailable_reason"] = "non_finite_projection"
    return projection
