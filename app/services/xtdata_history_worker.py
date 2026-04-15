"""Subprocess worker for blocking xtdata history calls."""

import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


def _ensure_xtdata(qmt_userdata_path: Optional[str]) -> Any:
    import xtquant.xtdata as xtdata  # type: ignore

    if qmt_userdata_path:
        xtdata.data_dir = os.path.join(qmt_userdata_path, "datadir")

    xtdata.enable_hello = False

    if hasattr(xtdata, "connect"):
        try:
            xtdata.connect()
        except Exception:
            pass

    return xtdata


def _format_market_data(data: Any) -> List[Dict[str, Any]]:
    if not data:
        return []

    formatted_data: List[Dict[str, Any]] = []

    if isinstance(data, dict) and data:
        first_field = list(data.keys())[0]
        first_df = data[first_field]

        if hasattr(first_df, "columns") and hasattr(first_df, "index"):
            stock_code = first_df.index[0] if len(first_df.index) > 0 else None
            if not stock_code:
                return []

            for date in list(first_df.columns):
                record: Dict[str, Any] = {}

                if "time" in data:
                    time_value = data["time"].loc[stock_code, date]
                    if isinstance(time_value, (int, float)) and time_value > 1000000000000:
                        record["time"] = datetime.fromtimestamp(time_value / 1000).strftime("%Y%m%d%H%M%S")
                    else:
                        record["time"] = str(time_value)
                else:
                    record["time"] = str(date)

                for field in [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "settlementPrice",
                    "openInterest",
                    "preClose",
                    "suspendFlag",
                ]:
                    if field not in data:
                        continue

                    value = data[field].loc[stock_code, date]
                    if hasattr(value, "item"):
                        value = value.item()

                    if field in {"volume", "openInterest", "suspendFlag"} and value is not None:
                        try:
                            value = int(value)
                        except Exception:
                            pass
                    elif value is not None:
                        try:
                            value = float(value)
                        except Exception:
                            pass

                    record[field] = value

                formatted_data.append(record)

    return formatted_data


def _action_get_local_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    xtdata = _ensure_xtdata(payload.get("qmt_userdata_path"))
    data = xtdata.get_local_data(
        field_list=payload.get("fields") or [],
        stock_list=[payload["stock_code"]],
        period=payload["period"],
        start_time=payload.get("start_time") or "",
        end_time=payload.get("end_time") or "",
        count=-1,
        dividend_type=payload.get("adjust_type") or "none",
    )
    return {"data": _format_market_data(data)}


def _action_get_full_kline(payload: Dict[str, Any]) -> Dict[str, Any]:
    xtdata = _ensure_xtdata(payload.get("qmt_userdata_path"))
    data = xtdata.get_full_kline(
        field_list=payload.get("fields") or [],
        stock_list=[payload["stock_code"]],
        period=payload["period"],
        start_time=payload.get("start_time") or "",
        end_time=payload.get("end_time") or "",
        count=1,
        dividend_type=payload.get("adjust_type") or "none",
    )
    return {"data": _format_market_data(data)}


def _action_download_history_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    xtdata = _ensure_xtdata(payload.get("qmt_userdata_path"))
    xtdata.download_history_data(
        stock_code=payload["stock_code"],
        period=payload["period"],
        start_time=payload.get("start_time") or "",
        end_time=payload.get("end_time") or "",
        incrementally=payload.get("incrementally"),
    )
    return {"message": f"download completed: {payload['stock_code']} {payload['period']}"}


def main() -> int:
    raw = sys.stdin.read().strip()
    if not raw:
        print(json.dumps({"ok": False, "error": "empty payload"}))
        return 2

    payload = json.loads(raw)
    action = payload.get("action")

    actions = {
        "get_local_data": _action_get_local_data,
        "get_full_kline": _action_get_full_kline,
        "download_history_data": _action_download_history_data,
    }

    if action not in actions:
        print(json.dumps({"ok": False, "error": f"unsupported action: {action}"}))
        return 2

    try:
        result = actions[action](payload)
        print(json.dumps({"ok": True, "result": result}, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())