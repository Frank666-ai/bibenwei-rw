# bibenwei-rw

Binance Portfolio Margin 币本位 RWUSD 双边对冲策略（独立实现）。

## 结构

```
backend/strategy/
├── coin_margined_rwusd_strategy.py   # 独立账户策略入口（启停、轮询、orchestration）
└── coin_margined_rwusd/              # 核心包（独立状态机/执行/资金/退出/持久化）
    ├── engine.py                     # 核心状态机：开仓、补仓、修复、止盈、恢复
    ├── execution.py                  # Binance 官方接口执行层（LIMIT GTX Maker、PAPI/SAPI）
    ├── exit.py / exit_execution.py   # 客户退出、赎回、结算、硬风险削减
    ├── capital_bootstrap.py          # 初始抵押识别与官方资本快照
    ├── capital_flow.py / coordinator.py / portfolio.py  # 资金账本与分配
    ├── market_features.py            # 30m RSI12 / ATR / 波动率特征（Wilder 平滑）
    └── ...
```

## 策略要点

- 仅支持 Binance Portfolio Margin 主网；BTC/ETH 单币单 lane，直接以原币（BTC/ETH）作为初始抵押。
- 同一 symbol 双边 Maker 对称开仓（Hedge Mode `LIMIT GTX`），缺边修复、对称补仓。
- 单侧止盈资格：ROE ≥ 150%（50x）、估算净利润 ≥ 1 USDC 且 ≥ 3 倍成本、30m 波动率门槛、RSI12 穿越（Wilder 平滑，30m 完成 K 线）。
- 止盈后先释放恢复单，利润链路 `PM -> Spot -> 转换 -> RWUSD 申购 -> PM 抵押` 异步推进。
- 保留硬风险削减与客户退出清算安全闭环；普通目标下降不机械减仓。

## 边界说明

- 仓库只含策略源码，不含配置、密钥、账户状态、运行时数据与部署脚本。
- 入口依赖宿主框架的账户管理与 Binance API 凭据注入，无法脱离宿主独立运行。
