"""Sequoia-X V2 主程序入口。

两种运行模式：
  python main.py               # 日常模式：8进程增量补数据 + 跑策略 + 飞书推送（2~3分钟）
  python main.py --backfill    # 回填模式：baostock 拉全市场历史K线（首次/补数据用，约12分钟）

指定股票池：
  通过环境变量 STOCK_POOL 指定股票池（逗号/顿号/分号/空格分隔）。
  仅当策略命中池内股票时才推送飞书；未设置时使用下方 _DEFAULT_STOCK_POOL。
  当前策略仅保留 3 个指定场景：
    - TurtleTradeStrategy  海龟突破（20日新高 + 成交额过亿 + 阳线防诱多）
    - MaVolumeStrategy     均线金叉 + 放量突破
    - HighTightFlagStrategy 高而窄的旗形整理突破
"""

import argparse
import os
import re
import sys

from dotenv import load_dotenv

load_dotenv()

from datetime import date  # noqa: E402

import socket  # noqa: E402

socket.setdefaulttimeout(10.0)

from sequoia_x.core.config import get_settings  # noqa: E402
from sequoia_x.core.logger import get_logger  # noqa: E402
from sequoia_x.data.engine import DataEngine  # noqa: E402
from sequoia_x.notify.feishu import FeishuNotifier  # noqa: E402
from sequoia_x.strategy.base import BaseStrategy  # noqa: E402
from sequoia_x.strategy.high_tight_flag import HighTightFlagStrategy  # noqa: E402
from sequoia_x.strategy.ma_volume import MaVolumeStrategy  # noqa: E402
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy  # noqa: E402

# 默认指定股票池（来自 Table_4933.xlsx 自选股中的 A 股代码）。
# 可用环境变量 STOCK_POOL 覆盖（分隔符支持 ，,、;； 及空白）。
_DEFAULT_STOCK_POOL = (
    "688825,688836,300017,300442,002929,002475,300691,002261,002352,300999,"
    "688981,603899,002032,002463,000568,002714,000538,002304,002415,601066,"
    "600009,601258,601390,601012,300015,600406,600276,603259,002050,300059,"
    "601100,300760,600900,601318,000333,300012,600036,603288,601888,300285,"
    "000651,600887,000001,601166,000858,600309,600519,600585,000338,601901,"
    "300014,002241,300750,300347,600438,002027,600031,600660,002594,002812,"
    "000725,002271"
)


def _load_stock_pool() -> set[str]:
    """解析指定股票池：优先环境变量 STOCK_POOL，否则使用默认池。"""
    raw = os.environ.get("STOCK_POOL", _DEFAULT_STOCK_POOL)
    tokens = re.split(r"[，,、;；\s]+", raw.strip())
    return {token for token in tokens if token}


def _change_pct(engine: DataEngine, symbol: str) -> float:
    """返回该股票最近一日涨跌幅（小数）。取不到时返回 0.0。"""
    try:
        df = engine.get_ohlcv(symbol)
        if len(df) < 2:
            return 0.0
        prev_close = float(df["close"].iloc[-2])
        if prev_close == 0:
            return 0.0
        return float(df["close"].iloc[-1]) / prev_close - 1.0
    except Exception:
        return 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Sequoia-X V2 选股系统")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="回填模式：通过 baostock 拉取全市场历史 K 线（约12分钟）",
    )
    args = parser.parse_args()

    try:
        # 1. 初始化配置
        settings = get_settings()

        # 2. 初始化日志
        logger = get_logger(__name__)
        logger.info("Sequoia-X V2 启动")

        # 指定股票池
        stock_pool = _load_stock_pool()
        logger.info(f"指定股票池：{len(stock_pool)} 只 -> {sorted(stock_pool)}")

        # 3. 初始化数据引擎
        engine = DataEngine(settings)

        if args.backfill:
            # ── 回填模式：单线程保守拉历史 K 线，自动多轮重跑 ──
            logger.info("进入回填模式...")
            all_symbols = engine.get_all_symbols()
            engine.backfill(all_symbols)
            logger.info("Sequoia-X V2 回填模式运行完成")
            return

        # ── 日常模式：单次 API 补今天 + 策略 + 推送 ──
        logger.info("开始拉取最新快照...")
        count = engine.sync_today_bulk()
        logger.info(f"快照同步完成，写入 {count} 只股票")

        # 4. 策略列表（仅保留指定场景：海龟突破 / 均线+放量 / 高而窄旗形）
        strategies: list[BaseStrategy] = [
            TurtleTradeStrategy(engine=engine, settings=settings),
            MaVolumeStrategy(engine=engine, settings=settings),
            HighTightFlagStrategy(engine=engine, settings=settings),
        ]

        notifier = FeishuNotifier(settings)

        # 5. 遍历策略，命中「指定股票池」的结果才推送至飞书
        for strategy in strategies:
            strategy_name = type(strategy).__name__
            logger.info(f"执行策略：{strategy_name}")

            selected: list[str] = strategy.run()
            # 仅保留池内股票
            selected = [symbol for symbol in selected if symbol in stock_pool]

            # 海龟突破：按当日涨幅从高到低排序
            if strategy_name == "TurtleTradeStrategy" and len(selected) > 1:
                selected.sort(key=lambda s: _change_pct(engine, s), reverse=True)

            logger.info(f"{strategy_name} 命中指定股票池 {len(selected)} 只股票")

            if selected:
                notifier.send(
                    symbols=selected,
                    strategy_name=strategy_name,
                    webhook_key=strategy.webhook_key,
                )
            else:
                logger.info(f"{strategy_name} 无池内命中，跳过推送")

    except Exception:
        try:
            _logger = get_logger(__name__)
            _logger.exception("主流程发生未捕获异常，程序终止")
        except Exception:
            import traceback

            traceback.print_exc()
        sys.exit(1)

    logger.info("Sequoia-X V2 运行完成")


if __name__ == "__main__":
    main()
