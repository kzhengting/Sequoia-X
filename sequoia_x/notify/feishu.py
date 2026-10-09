"""飞书通知模块：将选股结果通过 Webhook 推送至飞书群。"""

import json
from datetime import date

import requests

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger


logger = get_logger(__name__)


# 全市场代码->名称映射缓存（首次请求后复用，避免重复拉取）
_NAMES_CACHE: dict[str, str] | None = None


def _load_name_map() -> dict[str, str]:
    """加载并缓存全市场代码->名称映射。

    仅在首次访问时请求 AkShare；失败时缓存空表以避免重复请求。
    """
    global _NAMES_CACHE
    if _NAMES_CACHE is not None:
        return _NAMES_CACHE

    try:
        import akshare as ak

        df = ak.stock_info_a_code_name()
        _NAMES_CACHE = {
            str(code).zfill(6): str(name)
            for code, name in zip(df["code"], df["name"])
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"获取股票名称失败：{exc}")
        _NAMES_CACHE = {}

    return _NAMES_CACHE


class FeishuNotifier:
    """飞书 Webhook 推送器。

    根据策略的 webhook_key 路由到对应的飞书机器人。
    若 webhook_key 未在 Settings.strategy_webhooks 中配置，
    则 fallback 到 Settings.feishu_webhook_url。
    """

    # 策略类型 -> 类型备注（选股逻辑说明），用于在飞书卡片中标注本次选出的类型。
    STRATEGY_REMARKS: dict[str, str] = {
        "TurtleTradeStrategy": "海龟突破：20日新高 + 成交额过亿 + 阳线防诱多，按涨幅排序",
        "MaVolumeStrategy": "均线+放量突破",
        "HighTightFlagStrategy": "高而窄的旗形整理突破",
        "LimitUpShakeoutStrategy": "涨停洗盘回踩确认",
        "UptrendLimitDownStrategy": "上升趋势中的跌停反包",
        "RpsBreakoutStrategy": "欧奈尔 RPS 相对强度突破",
        "PrivatePlacementStrategy": "定向增发事件驱动",
    }

    def __init__(self, settings: Settings) -> None:
        """
        初始化 FeishuNotifier。

        Args:
            settings: Settings 实例，提供 Webhook URL 配置。
        """
        self.settings = settings

    @classmethod
    def _strategy_remark(cls, strategy_name: str) -> str:
        """返回策略类型对应的说明备注，兼容是否带 "Strategy" 后缀的写法。"""
        if strategy_name in cls.STRATEGY_REMARKS:
            return cls.STRATEGY_REMARKS[strategy_name]

        bare = strategy_name[:-8] if strategy_name.endswith("Strategy") else strategy_name
        for key, remark in cls.STRATEGY_REMARKS.items():
            bare_key = key[:-8] if key.endswith("Strategy") else key
            if bare_key == bare:
                return remark
        return "未收录策略类型"

    @staticmethod
    def _to_xueqiu_code(code: str) -> str:
        """将纯数字代码转为雪球格式：6开头→SH，4/8开头→BJ，其余→SZ。"""
        if code.startswith("6"):
            return f"SH{code}"
        elif code.startswith(("4", "8")):
            return f"BJ{code}"
        return f"SZ{code}"

    @staticmethod
    def _get_stock_names(symbols: list[str]) -> dict[str, str]:
        """通过 AkShare 批量查询股票名称，返回 {code: name} 映射（结果缓存）。"""
        name_map = _load_name_map()
        return {code: name_map[code] for code in symbols if code in name_map}

    def _build_card(self, symbols: list[str], strategy_name: str) -> dict:
        today = date.today().strftime("%Y-%m-%d")
        names = self._get_stock_names(symbols)
        remark = self._strategy_remark(strategy_name)

        links: list[str] = []
        for code in symbols:
            xq_code = self._to_xueqiu_code(code)
            name = names.get(code, xq_code)
            links.append(f"[{name}](https://xueqiu.com/S/{xq_code})")

        symbol_text = " ".join(links) if links else "（无选股结果）"

        return {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"📈 Sequoia-X 选股播报 | {strategy_name}",
                    },
                    "template": "blue",
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**日期：** {today}\n**策略：** {strategy_name}\n**类型备注：** {remark}\n**选股数量：** {len(symbols)}",
                        },
                    },
                    {"tag": "hr"},
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**选股列表：**\n{symbol_text}",
                        },
                    },
                ],
            },
        }

    def send(
        self,
        symbols: list[str],
        strategy_name: str,
        webhook_key: str = "default",
    ) -> None:
        """
        将选股结果格式化为飞书卡片消息并 POST 至对应 Webhook。

        根据 webhook_key 从 Settings 中查找专属 URL；
        若未配置，则 fallback 到 feishu_webhook_url。

        Args:
            symbols: 选股结果代码列表。
            strategy_name: 策略名称，用于卡片标题。
            webhook_key: 策略标识，用于路由到对应飞书机器人。

        Raises:
            不抛出异常，HTTP 失败时记录 ERROR 日志。
        """
        url = self.settings.get_webhook_url(webhook_key)
        payload = self._build_card(symbols, strategy_name)

        try:
            resp = requests.post(
                url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            # 解析飞书真正的返回体
            resp_json = resp.json()

            # 飞书真正的成功标志是内部的 code == 0
            if resp.status_code != 200 or resp_json.get("code") != 0:
                logger.error(
                    f"飞书推送失败 [{webhook_key}] "
                    f"HTTP状态={resp.status_code} 飞书响应={resp.text}"
                )
            else:
                logger.info(f"飞书推送成功 [{webhook_key}]，共 {len(symbols)} 只股票")

        except requests.RequestException as exc:
            logger.error(f"飞书推送请求异常 [{webhook_key}]：{exc}")
