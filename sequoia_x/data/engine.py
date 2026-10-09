"""数据引擎模块：负责 SQLite 行情数据存储与 AkShare 增量同步。

数据源：AkShare（新浪源 ``stock_zh_a_daily``），后复权（hfq）。
"""

import sqlite3
import time
from pathlib import Path

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    UNIQUE (symbol, date)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
"""


def _to_ak_symbol(symbol: str) -> str:
    """将纯数字代码转为 AkShare（新浪）格式：6/9→sh，4/8→bj，其余→sz。"""
    if symbol.startswith(("6", "9")):
        prefix = "sh"
    elif symbol.startswith(("4", "8")):
        prefix = "bj"
    else:
        prefix = "sz"
    return f"{prefix}{symbol}"


def _fetch_daily(
    ak_symbol: str,
    start: str,
    end: str,
    adjust: str,
    retries: int = 3,
) -> pd.DataFrame:
    """通过 AkShare（新浪源）拉取单只股票日 K 线，带指数退避重试。

    Args:
        ak_symbol: 带市场前缀的代码，如 ``sh600519``。
        start: 起始日期 ``YYYY-MM-DD``。
        end: 结束日期 ``YYYY-MM-DD``。
        adjust: ``"hfq"``（后复权）/ ``""``（不复权）。
        retries: 失败重试次数。

    Returns:
        原始 sina 日 K DataFrame；失败则抛出最后一次异常。
    """
    import akshare as ak

    last_exc: Exception | None = None
    for attempt in range(max(1, retries)):
        try:
            df = ak.stock_zh_a_daily(
                symbol=ak_symbol,
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
                adjust=adjust,
            )
            return df if df is not None else pd.DataFrame()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
    if last_exc is not None:
        raise last_exc
    return pd.DataFrame()


def _normalize(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """把 sina 日 K 归一化为 stock_daily 契约。

    契约列：``symbol, date, open, high, low, close, volume, turnover``。
    ``turnover`` 列取 sina 的 ``amount``（成交额），与旧 baostock 口径一致。
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    # sina 返回自带 turnover(换手率) 列，先丢弃避免与成交额重名
    if "amount" in df.columns and "turnover" in df.columns:
        df = df.drop(columns=["turnover"])
    df = df.rename(columns={"amount": "turnover"})

    keep = ["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]
    for col in ["open", "high", "low", "close", "volume", "turnover"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["close"])
    if "volume" in df.columns:
        df = df[df["volume"] > 0]

    df["symbol"] = symbol
    for col in keep:
        if col not in df.columns:
            df[col] = None
    return df[keep]


def _ak_fetch_batch(tasks: list) -> list:
    """多进程 worker：批量拉取 AkShare 后复权日 K 数据。"""
    frames = []
    for symbol, ak_symbol, start, end in tasks:
        try:
            raw = _fetch_daily(ak_symbol, start, end, "hfq")
            norm = _normalize(raw, symbol)
            if not norm.empty:
                frames.append(norm)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[{symbol}] 拉取失败: {exc}")
            continue
    return frames


class DataEngine:
    """行情数据引擎，负责 SQLite 存储和 AkShare 数据同步。"""

    def __init__(self, settings: Settings) -> None:
        self.db_path: str = settings.db_path
        self.start_date: str = settings.start_date
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.commit()
        logger.info(f"数据库初始化完成：{self.db_path}")

    def _get_last_date(self, symbol: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM stock_daily WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(
                "SELECT * FROM stock_daily WHERE symbol = ? ORDER BY date",
                conn,
                params=(symbol,),
            )
        return df

    @staticmethod
    def _to_ak_symbol(symbol: str) -> str:
        """将纯数字代码转为 AkShare（新浪）格式：6/9→sh，4/8→bj，其余→sz。"""
        return _to_ak_symbol(symbol)

    # 向后兼容旧命名（baostock 时代遗留）
    _to_baostock_code = _to_ak_symbol

    # ── 数据同步 ──

    def sync_today_bulk(self) -> int:
        """多进程并行通过 AkShare 拉取增量数据（后复权），写入 SQLite。"""
        from datetime import date, timedelta
        from multiprocessing import Pool

        today_str = date.today().strftime("%Y-%m-%d")

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol"
            ).fetchall()

        if not rows:
            logger.warning("本地无股票数据，请先执行 --backfill")
            return 0

        tasks = []
        for symbol, last_date in rows:
            if last_date and last_date >= today_str:
                continue
            start = today_str
            if last_date:
                start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            tasks.append((symbol, _to_ak_symbol(symbol), start, today_str))

        if not tasks:
            logger.info("所有股票已是最新，无需更新")
            return 0

        logger.info(f"需要更新 {len(tasks)} 只股票，启动多进程并行拉取...")

        n_workers = min(8, len(tasks))
        chunks = [tasks[i::n_workers] for i in range(n_workers)]

        with Pool(n_workers) as pool:
            batch_results = pool.map(_ak_fetch_batch, chunks)

        frames = [frame for batch in batch_results for frame in batch]

        if not frames:
            logger.info("无新数据（可能非交易日）")
            return 0

        df = pd.concat(frames, ignore_index=True)
        count = len(df)
        with sqlite3.connect(self.db_path) as conn:
            for d in df["date"].unique().tolist():
                conn.execute("DELETE FROM stock_daily WHERE date = ?", (d,))
            df.to_sql(
                "stock_daily", conn, if_exists="append",
                index=False, method="multi", chunksize=500,
            )
            conn.commit()

        logger.info(f"sync_today_bulk: 写入 {count} 条数据")
        return count

    def backfill(self, symbols: list[str]) -> None:
        """通过 AkShare 多进程并行批量回填历史日 K 线数据（后复权）。

        容错机制：
        - 多进程并行拉取（默认 8 进程），单只失败自动重试 3 次
        - 已入库的自动 skip，中断后可重跑续传
        - 边拉边写：每个分片完成即落库，降低内存占用
        """
        from datetime import date, timedelta
        from multiprocessing import Pool

        today_str = date.today().strftime("%Y-%m-%d")

        tasks = []
        skipped = 0
        for symbol in symbols:
            last_date = self._get_last_date(symbol)
            if last_date and last_date >= today_str:
                skipped += 1
                continue
            start = last_date or self.start_date
            if last_date:
                start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            tasks.append((symbol, _to_ak_symbol(symbol), start, today_str))

        logger.info(
            f"需回填 {len(tasks)} 只（已有数据跳过 {skipped} 只），启动多进程并行拉取..."
        )

        if not tasks:
            logger.info("所有股票均已有最新数据，无需回填")
            return

        n_workers = min(8, len(tasks))
        chunks = [tasks[i::n_workers] for i in range(n_workers)]

        n_rows = 0
        n_syms = 0
        with Pool(n_workers) as pool:
            for frames in pool.imap_unordered(_ak_fetch_batch, chunks):
                if not frames:
                    continue
                df = pd.concat(frames, ignore_index=True)
                df = df.drop_duplicates(subset=["symbol", "date"])
                with sqlite3.connect(self.db_path) as conn:
                    df.to_sql(
                        "stock_daily", conn, if_exists="append",
                        index=False, method="multi", chunksize=1000,
                    )
                    conn.commit()
                n_rows += len(df)
                n_syms += len(frames)
                logger.info(f"回填进度 — 已获取 {n_syms} 只 / {n_rows} 行")

        logger.info(
            f"回填完成 — 写入 {n_rows} 行，覆盖 {n_syms} 只（跳过 {skipped} 只）"
        )

    # ── 股票列表 ──

    def get_all_symbols(self) -> list[str]:
        """通过 AkShare 获取全市场 A 股代码列表。"""
        import akshare as ak

        try:
            df = ak.stock_info_a_code_name()
            symbols = [str(code).zfill(6) for code in df["code"].tolist()]
            logger.info(f"获取股票列表完成，共 {len(symbols)} 只")
            return symbols
        except Exception as e:  # noqa: BLE001
            logger.error(f"获取股票列表失败: {e}")
            return []

    def get_local_symbols(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily"
            ).fetchall()
        return [row[0] for row in rows]
