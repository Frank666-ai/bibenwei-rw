"""Narrow Binance environment adapters for RWUSD."""

from __future__ import annotations

from typing import List, Optional


class ReadOnlyBinanceView:
    """Expose only the Binance reads required by the RWUSD simulator."""

    def __init__(self, client: object) -> None:
        self._client = client
        self.exchange = str(
            getattr(client, "exchange", "") or ""
        ).strip().lower()
        self.is_testnet = bool(getattr(client, "is_testnet", False))

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 20,
    ):
        return await self._client.get_klines(symbol, interval, limit=limit)

    async def get_ticker(self, symbol: str):
        return await self._client.get_ticker(symbol)

    async def get_book_ticker(self, symbol: str):
        return await self._client.get_book_ticker(symbol)

    async def get_order_book(self, symbol: str, limit: int = 20):
        return await self._client.get_order_book(symbol, limit=limit)

    async def get_premium_index(self, symbol: str):
        return await self._client.get_premium_index(symbol)

    async def get_account_info(self):
        return await self._client.get_account_info()

    async def get_positions(self):
        return await self._client.get_positions()

    async def get_portfolio_positions(self):
        return await self.get_positions()


class BinanceLocalDryRunClient(ReadOnlyBinanceView):
    """Compatibility wrapper for the legacy account-manager dry-run path."""

    def __init__(
        self,
        client: object,
        simulation_capital: float = 0.0,
    ) -> None:
        super().__init__(client)
        self.simulation_capital = max(0.0, float(simulation_capital or 0.0))


class BinanceRWUSDExecutionView(ReadOnlyBinanceView):
    """Expose only environment-routed methods required by RWUSD."""

    def __init__(self, client: object) -> None:
        super().__init__(client)

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 20,
    ):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_klines(
                symbol,
                interval,
                limit=limit,
            )
        return await super().get_klines(symbol, interval, limit=limit)

    async def get_ticker(self, symbol: str):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_ticker(symbol)
        return await super().get_ticker(symbol)

    async def get_book_ticker(self, symbol: str):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_book_ticker(symbol)
        return await super().get_book_ticker(symbol)

    async def get_order_book(self, symbol: str, limit: int = 20):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_order_book(
                symbol,
                limit=limit,
            )
        return await super().get_order_book(symbol, limit=limit)

    async def get_premium_index(self, symbol: str):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_premium_index(symbol)
        return await super().get_premium_index(symbol)

    async def get_account_info(self):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_account()
        return await self._client.get_rwusd_mainnet_account()

    async def get_positions(self):
        return await self.get_um_positions()

    async def get_portfolio_positions(self):
        um_positions = await self.get_um_positions()
        if self.is_testnet:
            return um_positions
        cm_positions = await self._client.get_rwusd_mainnet_cm_positions()
        return [*um_positions, *cm_positions]

    async def get_um_positions(self):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_um_positions()
        return await self._client.get_rwusd_mainnet_um_positions()

    async def get_um_position_mode(self):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_um_position_mode()
        return await self._client.get_rwusd_mainnet_um_position_mode()

    async def set_um_position_mode(self, dual_side: bool):
        if self.is_testnet:
            return await self._client.set_rwusd_testnet_um_position_mode(
                dual_side
            )
        return await self._client.set_rwusd_mainnet_um_position_mode(
            dual_side
        )

    async def set_um_leverage(self, symbol: str, leverage: int):
        if self.is_testnet:
            return await self._client.set_rwusd_testnet_um_leverage(
                symbol,
                leverage,
            )
        return await self._client.set_rwusd_mainnet_um_leverage(
            symbol,
            leverage,
        )

    async def get_um_symbol_config(self, symbol: str):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_um_symbol_config(
                symbol
            )
        return await self._client.get_rwusd_mainnet_um_symbol_config(symbol)

    async def get_um_leverage_brackets(self, symbol: str):
        if self.is_testnet:
            return await (
                self._client.get_rwusd_testnet_um_leverage_brackets(symbol)
            )
        return await self._client.get_rwusd_mainnet_um_leverage_brackets(
            symbol
        )

    async def get_um_open_orders(
        self,
        *,
        symbol: Optional[str] = None,
    ):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_um_open_orders(
                symbol=symbol
            )
        return await self._client.get_rwusd_mainnet_um_open_orders(
            symbol=symbol
        )

    async def get_um_order(
        self,
        *,
        symbol: str,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
    ):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_um_order(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
            )
        return await self._client.get_rwusd_mainnet_um_order(
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_order_id,
        )

    async def cancel_um_order(self, *, symbol: str, order_id: str):
        if self.is_testnet:
            return await self._client.cancel_rwusd_testnet_um_order(
                symbol=symbol,
                order_id=order_id,
            )
        return await self._client.cancel_rwusd_mainnet_um_order(
            symbol=symbol,
            order_id=order_id,
        )

    async def normalize_um_limit_order_quantity(
        self,
        symbol: str,
        quantity: float,
        reference_price: float,
    ):
        if self.is_testnet:
            return await (
                self._client.normalize_rwusd_testnet_limit_order_quantity(
                    symbol,
                    quantity,
                    reference_price,
                )
            )
        return await (
            self._client.normalize_rwusd_mainnet_limit_order_quantity(
                symbol,
                quantity,
                reference_price,
            )
        )

    async def get_um_minimum_limit_order_quantity(
        self,
        symbol: str,
        reference_price: float,
    ):
        if self.is_testnet:
            return await (
                self._client.get_rwusd_testnet_minimum_limit_order_quantity(
                    symbol,
                    reference_price,
                )
            )
        return await (
            self._client.get_rwusd_mainnet_minimum_limit_order_quantity(
                symbol,
                reference_price,
            )
        )

    async def normalize_um_market_close_quantity(
        self,
        symbol: str,
        quantity: float,
        reference_price: float,
    ):
        if self.is_testnet:
            return await (
                self._client.normalize_rwusd_testnet_market_order_quantity(
                    symbol,
                    quantity,
                    reference_price,
                    enforce_min_notional=False,
                    cap_to_maximum=False,
                )
            )
        return await (
            self._client.normalize_rwusd_mainnet_market_order_quantity(
                symbol,
                quantity,
                reference_price,
                enforce_min_notional=False,
                cap_to_maximum=False,
            )
        )

    async def normalize_um_post_only_price(
        self,
        *,
        symbol: str,
        side: str,
        best_bid: float,
        best_ask: float,
        price_mode: str,
    ):
        if self.is_testnet:
            return await (
                self._client.normalize_rwusd_testnet_post_only_price(
                    symbol=symbol,
                    side=side,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    price_mode=price_mode,
                )
            )
        return await self._client.normalize_rwusd_mainnet_post_only_price(
            symbol=symbol,
            side=side,
            best_bid=best_bid,
            best_ask=best_ask,
            price_mode=price_mode,
        )

    async def submit_um_market_order(self, **kwargs):
        if self.is_testnet:
            return await self._client.submit_rwusd_testnet_um_market_order(
                **kwargs
            )
        return await self._client.submit_rwusd_mainnet_um_market_order(
            **kwargs
        )

    async def submit_um_post_only_order(self, **kwargs):
        if self.is_testnet:
            return await (
                self._client.submit_rwusd_testnet_um_post_only_order(**kwargs)
            )
        return await self._client.submit_rwusd_mainnet_um_post_only_order(
            **kwargs
        )

    async def submit_um_limit_close_order(self, **kwargs):
        if self.is_testnet:
            return await (
                self._client.submit_rwusd_testnet_um_limit_close_order(
                    **kwargs
                )
            )
        return await self._client.submit_rwusd_mainnet_um_limit_close_order(
            **kwargs
        )

    async def get_um_commission_rate(self, symbol: str):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_um_commission_rate(
                symbol
            )
        return await self._client.get_rwusd_mainnet_um_commission_rate(symbol)

    async def get_um_user_trades(
        self,
        symbol: str,
        *,
        from_id: Optional[int] = None,
        start_time_ms: Optional[int] = None,
        limit: int = 1000,
    ):
        if self.is_testnet:
            return await self._client.get_rwusd_testnet_um_user_trades(
                symbol,
                from_id=from_id,
                start_time_ms=start_time_ms,
                limit=limit,
            )
        return await self._client.get_rwusd_mainnet_um_user_trades(
            symbol,
            from_id=from_id,
            start_time_ms=start_time_ms,
            limit=limit,
        )

    async def get_rwusd_account(self):
        if self.is_testnet:
            raise RuntimeError("RWUSD account is unavailable on testnet")
        return await self._client.get_rwusd_account()

    async def get_rwusd_rate_history(
        self,
        *,
        current: int = 1,
        size: int = 1,
    ):
        if self.is_testnet:
            raise RuntimeError(
                "RWUSD rate history is unavailable on testnet"
            )
        return await self._client.get_rwusd_rate_history(
            current=current,
            size=size,
        )

    async def get_rwusd_quota(self):
        if self.is_testnet:
            raise RuntimeError("RWUSD quota is unavailable on testnet")
        return await self._client.get_rwusd_quota()

    async def get_api_key_permissions(self):
        if self.is_testnet:
            raise RuntimeError(
                "API key permission inspection is unavailable on testnet"
            )
        return await self._client.get_rwusd_api_key_permissions()

    async def get_api_trading_status(self):
        if self.is_testnet:
            raise RuntimeError(
                "API trading status inspection is unavailable on testnet"
            )
        return await self._client.get_rwusd_api_trading_status()

    async def transfer_pm_to_spot(self, *, asset: str, amount: float):
        if self.is_testnet:
            raise RuntimeError(
                "Portfolio Margin transfer is unavailable on testnet"
            )
        return await self._client.transfer_rwusd_portfolio_margin_to_spot(
            asset=asset,
            amount=amount,
        )

    async def transfer_spot_to_pm(self, *, asset: str, amount: float):
        if self.is_testnet:
            raise RuntimeError(
                "Portfolio Margin transfer is unavailable on testnet"
            )
        return await self._client.transfer_coin_asset_spot_to_portfolio_margin(
            asset=asset,
            amount=amount,
        )

    async def collect_pm_asset(self, *, asset: str):
        if self.is_testnet:
            raise RuntimeError(
                "Portfolio Margin asset collection is unavailable on testnet"
            )
        return await self._client.collect_rwusd_portfolio_margin_asset(
            asset=asset
        )

    async def get_pm_to_spot_transfer_history(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ):
        if self.is_testnet:
            raise RuntimeError(
                "Portfolio Margin transfer is unavailable on testnet"
            )
        return await (
            self._client.get_rwusd_portfolio_margin_transfer_history(
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
        )

    async def get_spot_to_pm_transfer_history(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ):
        if self.is_testnet:
            raise RuntimeError(
                "Portfolio Margin transfer is unavailable on testnet"
            )
        return await (
            self._client.get_rwusd_spot_to_portfolio_margin_transfer_history(
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
        )

    async def get_spot_asset_balance(self, asset: str):
        if self.is_testnet:
            raise RuntimeError("Spot balance is unavailable on testnet")
        return await self._client.get_spot_asset_balance(asset)

    async def get_spot_asset_balances(self, assets: List[str]):
        if self.is_testnet:
            raise RuntimeError("Spot balance is unavailable on testnet")
        return await self._client.get_spot_asset_balances(assets)

    async def get_funding_asset_balance(self, asset: str):
        if self.is_testnet:
            raise RuntimeError("Funding balance is unavailable on testnet")
        return await self._client.get_funding_asset_balance(asset)

    async def get_funding_asset_balances(self, assets: List[str]):
        if self.is_testnet:
            raise RuntimeError("Funding balance is unavailable on testnet")
        return await self._client.get_funding_asset_balances(assets)

    async def transfer_funding_to_spot(
        self,
        *,
        asset: str,
        amount: float,
    ):
        if self.is_testnet:
            raise RuntimeError("Funding transfer is unavailable on testnet")
        return await self._client.transfer_coin_asset_funding_to_spot(
            asset=asset,
            amount=amount,
        )

    async def get_funding_to_spot_transfer_history(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ):
        if self.is_testnet:
            raise RuntimeError(
                "Funding transfer history is unavailable on testnet"
            )
        return await self._client.get_funding_to_spot_transfer_history(
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    async def subscribe_rwusd(self, *, asset: str, amount: float):
        if self.is_testnet:
            raise RuntimeError(
                "RWUSD subscription is unavailable on testnet"
            )
        return await self._client.subscribe_rwusd(
            asset=asset,
            amount=amount,
        )

    async def get_convert_exchange_info(
        self,
        *,
        from_asset: str,
        to_asset: str,
    ):
        if self.is_testnet:
            raise RuntimeError("Binance Convert is unavailable on testnet")
        return await self._client.get_convert_exchange_info(
            from_asset=from_asset,
            to_asset=to_asset,
        )

    async def get_convert_quote(
        self,
        *,
        from_asset: str,
        to_asset: str,
        from_amount: float,
    ):
        if self.is_testnet:
            raise RuntimeError("Binance Convert is unavailable on testnet")
        return await self._client.get_convert_quote(
            from_asset=from_asset,
            to_asset=to_asset,
            from_amount=from_amount,
        )

    async def accept_convert_quote(self, *, quote_id: str):
        if self.is_testnet:
            raise RuntimeError("Binance Convert is unavailable on testnet")
        return await self._client.accept_convert_quote(quote_id=quote_id)

    async def get_convert_order_status(
        self,
        *,
        order_id: Optional[str] = None,
        quote_id: Optional[str] = None,
    ):
        if self.is_testnet:
            raise RuntimeError("Binance Convert is unavailable on testnet")
        return await self._client.get_convert_order_status(
            order_id=order_id,
            quote_id=quote_id,
        )

    async def get_rwusd_subscription_history(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int,
        asset: Optional[str] = None,
    ):
        if self.is_testnet:
            raise RuntimeError("RWUSD history is unavailable on testnet")
        return await self._client.get_rwusd_subscription_history(
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            asset=asset,
        )

    async def get_rwusd_redemption_history(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ):
        if self.is_testnet:
            raise RuntimeError(
                "RWUSD redemption history is unavailable on testnet"
            )
        return await self._client.get_rwusd_redemption_history(
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    async def get_deposit_history(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int,
        offset: int = 0,
        limit: int = 1000,
    ):
        if self.is_testnet:
            raise RuntimeError(
                "Deposit history is unavailable on testnet"
            )
        return await self._client.get_deposit_history(
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            offset=offset,
            limit=limit,
        )

    async def redeem_rwusd(
        self,
        *,
        amount: float,
        redeem_type: str,
    ):
        if self.is_testnet:
            raise RuntimeError(
                "RWUSD redemption is unavailable on testnet"
            )
        return await self._client.redeem_rwusd(
            amount=amount,
            redeem_type=redeem_type,
        )

    async def get_rwusd_collateral_balance(self):
        if self.is_testnet:
            raise RuntimeError(
                "RWUSD Portfolio Margin collateral is unavailable on testnet"
            )
        return await (
            self._client.get_rwusd_portfolio_margin_collateral_balance(
                "RWUSD"
            )
        )

    async def get_rwusd_transferable_balance(self):
        if self.is_testnet:
            raise RuntimeError(
                "RWUSD Portfolio Margin transferable balance is unavailable "
                "on testnet"
            )
        return await (
            self._client.get_rwusd_portfolio_margin_transferable_balance()
        )

    async def get_rwusd_exit_transferable_balance(self):
        if self.is_testnet:
            raise RuntimeError(
                "RWUSD Portfolio Margin exit balance is unavailable on testnet"
            )
        return await (
            self._client.get_rwusd_portfolio_margin_transferable_balance(
                transfer_type="FUTURE_TO_EARN"
            )
        )

    async def get_margin_asset_balance(self, asset: str):
        target = str(asset or "").strip().upper()
        if self.is_testnet:
            account = await self._client.get_rwusd_testnet_account()
            return {
                "asset": target,
                "wallet_balance": max(
                    0.0,
                    float(
                        (account or {}).get("source_wallet_balance")
                        or (account or {}).get("available")
                        or 0
                    ),
                ),
                "um_wallet_balance": max(
                    0.0,
                    float(
                        (account or {}).get("source_wallet_balance")
                        or (account or {}).get("available")
                        or 0
                    ),
                ),
                "raw": (account or {}).get("raw"),
            }
        balance = await (
            self._client.get_rwusd_portfolio_margin_collateral_balance(target)
        )
        loader = getattr(
            self._client,
            "get_rwusd_portfolio_margin_max_withdraw",
            None,
        )
        if loader is None:
            return balance
        # Portfolio Margin withdraw capacity changes with unrealized PnL.
        maximum = max(0.0, float(await loader(asset=target) or 0))
        result = dict(balance or {})
        result["max_withdraw_balance"] = maximum
        result["transferable_balance"] = min(
            max(
                0.0,
                float(result.get("transferable_balance", 0) or 0),
            ),
            maximum,
        )
        return result

    async def get_margin_asset_settlement(self, asset: str):
        if self.is_testnet:
            raise RuntimeError(
                "Portfolio Margin settlement balance is unavailable on testnet"
            )
        return await (
            self._client.get_rwusd_portfolio_margin_signed_balance(asset)
        )

    async def get_margin_asset_max_withdraw(self, asset: str) -> float:
        if self.is_testnet:
            raise RuntimeError(
                "Portfolio Margin max withdraw is unavailable on testnet"
            )
        return await self._client.get_rwusd_portfolio_margin_max_withdraw(
            asset=asset
        )

    async def repay_futures_negative_balance(
        self,
        *,
        source: str | None = None,
    ):
        if self.is_testnet:
            raise RuntimeError(
                "Futures negative balance repayment is unavailable on testnet"
            )
        return await self._client.repay_rwusd_futures_negative_balance(
            source=source
        )

    async def repay_margin_debt(self, *, asset: str, amount: float):
        if self.is_testnet:
            raise RuntimeError(
                "Margin debt repayment is unavailable on testnet"
            )
        return await self._client.repay_rwusd_margin_debt(
            asset=asset,
            amount=amount,
        )

    async def transfer_rwusd_to_portfolio_margin(self, *, amount: float):
        if self.is_testnet:
            raise RuntimeError(
                "RWUSD Portfolio Margin collateral transfer is unavailable "
                "on testnet"
            )
        return await self._client.transfer_rwusd_to_portfolio_margin(
            amount=amount
        )

    async def transfer_rwusd_from_portfolio_margin(
        self,
        *,
        amount: float,
    ):
        if self.is_testnet:
            raise RuntimeError(
                "RWUSD Portfolio Margin exit transfer is unavailable on "
                "testnet"
            )
        return await self._client.transfer_rwusd_from_portfolio_margin(
            amount=amount
        )
