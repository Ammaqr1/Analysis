from collections import defaultdict
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from db import (
    get_buy_order_details_sync,
    get_data_for_order_execution_sync,
    get_sell_order_details_sync,
)

st.set_page_config(page_title="Trading DB Visualization", layout="wide")
st.title("Trading Database Visualization")
st.caption("Tables: SellOrderDetails, BuyOrderDetails, DataForOrderExecution")

IST = ZoneInfo("Asia/Kolkata")


def _parse_dt(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    # Treat naive datetimes as UTC since DB createdAt is UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt


def _created_at_ist_display(value: object) -> str:
    dt = _parse_dt(value)
    if dt is None:
        return str(value or "")
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")


@st.cache_data(ttl=5)
def load_table_data() -> tuple[list[dict], list[dict], list[dict]]:
    """Load rows from Postgres via db.py helpers. Returns (sell, buy, execution)."""
    sell_rows = get_sell_order_details_sync()
    buy_rows = get_buy_order_details_sync()
    exec_rows = get_data_for_order_execution_sync()
    for row in sell_rows:
        row["_createdAt_raw"] = row.get("createdAt")
        row["createdAt"] = _created_at_ist_display(row.get("_createdAt_raw"))
    for row in buy_rows:
        row["_createdAt_raw"] = row.get("createdAt")
        row["createdAt"] = _created_at_ist_display(row.get("_createdAt_raw"))
    # Display execution createdAt in IST while keeping raw UTC for filtering.
    for row in exec_rows:
        row["_createdAt_raw"] = row.get("createdAt")
        row["createdAt"] = _created_at_ist_display(row.get("_createdAt_raw"))
    return sell_rows, buy_rows, exec_rows


def to_dataframe(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _parent_exec_id(row: dict) -> str | None:
    """FK column from Prisma / Postgres (quoted camelCase)."""
    v = row.get("dataForOrderExecutionId")
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    return str(v)


def build_session_view(
    exec_rows: list[dict],
    buy_rows: list[dict],
    sell_rows: list[dict],
) -> tuple[list[tuple[dict, list[dict], list[dict]]], list[dict], list[dict]]:
    """
    Group buys/sells under each execution row.
    Returns (sessions_ordered, orphan_buys, orphan_sells).
    """
    buys_by_parent: dict[str, list[dict]] = defaultdict(list)
    for b in buy_rows:
        pid = _parent_exec_id(b)
        if pid:
            buys_by_parent[pid].append(b)

    sells_by_parent: dict[str, list[dict]] = defaultdict(list)
    for s in sell_rows:
        pid = _parent_exec_id(s)
        if pid:
            sells_by_parent[pid].append(s)

    orphan_buys = [b for b in buy_rows if _parent_exec_id(b) is None]
    orphan_sells = [s for s in sell_rows if _parent_exec_id(s) is None]

    exec_map = {r["id"]: r for r in exec_rows}
    sessions: list[tuple[dict, list[dict], list[dict]]] = []
    for ex in exec_rows:
        eid = ex["id"]
        sessions.append((ex, buys_by_parent.get(eid, []), sells_by_parent.get(eid, [])))

    # Linked orders whose parent id is missing from execution table
    missing_parent_ids = (
        set(buys_by_parent) | set(sells_by_parent)
    ) - set(exec_map)
    for mpid in sorted(missing_parent_ids):
        ghost = {
            "id": mpid,
            "started_time": "—",
            "at_the_money_time": "—",
            "note": "Orphan link (no matching DataForOrderExecution row)",
        }
        sessions.append((ghost, buys_by_parent[mpid], sells_by_parent[mpid]))

    return sessions, orphan_buys, orphan_sells


EXEC_SUMMARY_COLS = [
    "createdAt",
    "started_time",
    "at_the_money_time",
    "at_the_money_price",
    "exchange",
    "expiry_date",
    "ce_ik",
    "pe_ik",
    "high_price_ce",
    "high_price_time_ce",
    "high_price_pe",
    "high_price_time_pe",
    "high_price_start_time",
    "high_price_end_time",
]


def _exec_created_at_date(row: dict) -> date | None:
    """Parse DataForOrderExecution.createdAt for date-range filtering."""
    v = row.get("_createdAt_raw", row.get("createdAt"))
    dt = _parse_dt(v)
    if dt is None:
        if isinstance(v, date):
            return v
        return None
    return dt.astimezone(IST).date()


def _filter_sessions_by_exec_created_at(
    sessions: list[tuple[dict, list[dict], list[dict]]],
    start_d: date,
    end_d: date,
    include_orphan_linked: bool,
) -> list[tuple[dict, list[dict], list[dict]]]:
    """Keep sessions whose execution row's createdAt falls in [start_d, end_d]."""
    out: list[tuple[dict, list[dict], list[dict]]] = []
    for ex, buys, sells in sessions:
        if ex.get("note") == "Orphan link (no matching DataForOrderExecution row)":
            if include_orphan_linked:
                out.append((ex, buys, sells))
            continue
        d = _exec_created_at_date(ex)
        if d is None:
            out.append((ex, buys, sells))
            continue
        if start_d <= d <= end_d:
            out.append((ex, buys, sells))
    return out

BUY_DISPLAY_COLS = [
    "createdAt",
    "user",
    "type",
    "order_id",
    "o_status",
    "r_quantity",
    "r_buy_price",
    "o_buyed_quantity",
    "m_quantity",
    "m_price",
    "m_result",
    "m_buyed_quantity",
]

SELL_DISPLAY_COLS = [
    "createdAt",
    "user",
    "type",
    "order_id",
    "mode",
    "o_status",
    "r_quantity",
    "o_buyed_quantity",
    "stop_loss_price",
    "target_price",
    "m_quantity",
    "m_price",
    "m_result",
]


def _project_columns(df: pd.DataFrame, preferred: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    cols = [c for c in preferred if c in df.columns]
    rest = [c for c in df.columns if c not in cols and c != "dataForOrderExecutionId"]
    ordered = cols + rest
    return df[ordered]


def _with_sell_mode_indicator(df: pd.DataFrame) -> pd.DataFrame:
    """Add a color indicator column for target/stoploss mode."""
    if df.empty or "mode" not in df.columns:
        return df

    def _label(mode: object) -> str:
        mode_text = str(mode or "").strip().lower()
        if mode_text.startswith("target"):
            return "🟢 TARGET"
        if mode_text.startswith("stop"):
            return "🔴 STOPLOSS"
        return "⚪ OTHER"

    out = df.copy()
    out.insert(0, "signal", out["mode"].map(_label))
    return out


def render_linked_overview_tab(
    exec_rows: list[dict],
    buy_rows: list[dict],
    sell_rows: list[dict],
) -> None:
    st.subheader("Sessions: execution snapshot → buy & sell legs")
    st.caption(
        "Each block is one `DataForOrderExecution` row with orders that reference it via "
        "`dataForOrderExecutionId`. Filter uses `createdAt` on each execution row "
        "(same source as `get_data_for_order_execution_sync`)."
    )

    if not exec_rows and not buy_rows and not sell_rows:
        st.info("No data in any table.")
        return

    sessions, orphan_buys, orphan_sells = build_session_view(
        exec_rows, buy_rows, sell_rows
    )

    if not sessions and not orphan_buys and not orphan_sells:
        st.info("No sessions to show.")
        return

    exec_dates = [
        d for r in exec_rows if (d := _exec_created_at_date(r)) is not None
    ]
    if exec_dates:
        default_min, default_max = min(exec_dates), max(exec_dates)
    else:
        default_min = default_max = date.today()

    f1, f2, f3 = st.columns([1, 1, 2])
    with f1:
        start_filter = st.date_input(
            "Execution created from",
            value=default_min,
            min_value=default_min,
            max_value=default_max,
            key="linked_exec_created_from",
        )
    with f2:
        end_filter = st.date_input(
            "Execution created to",
            value=default_max,
            min_value=default_min,
            max_value=default_max,
            key="linked_exec_created_to",
        )
    with f3:
        include_orphan_linked = st.checkbox(
            "Include orphan-linked sessions (buy/sell points to missing execution row)",
            value=True,
            key="linked_include_orphan_exec",
        )

    start_d, end_d = start_filter, end_filter
    if start_d > end_d:
        st.warning("`from` is after `to`; swap or widen the range.")
        start_d, end_d = end_d, start_d

    sessions_filtered = _filter_sessions_by_exec_created_at(
        sessions, start_d, end_d, include_orphan_linked
    )

    st.metric("Execution snapshots (DB total)", len(exec_rows))
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Buy rows (total)", len(buy_rows))
    with c2:
        st.metric("Sell rows (total)", len(sell_rows))
    with c3:
        linked_buys = len(buy_rows) - len(orphan_buys)
        linked_sells = len(sell_rows) - len(orphan_sells)
        st.metric("Linked buy / sell", f"{linked_buys} / {linked_sells}")

    st.metric(
        "Sessions in date range",
        len(sessions_filtered),
        help=f"Execution `createdAt` between {start_d} and {end_d} (inclusive).",
    )

    if not sessions_filtered:
        st.info("No sessions match the selected `createdAt` range.")
        if orphan_buys or orphan_sells:
            st.divider()
            st.subheader("Unlinked orders")
            st.caption(
                "Rows with empty `dataForOrderExecutionId` (legacy runs or manual inserts)."
            )
            if orphan_buys:
                st.markdown("**Buy (unlinked)**")
                st.dataframe(
                    _project_columns(to_dataframe(orphan_buys), BUY_DISPLAY_COLS),
                    use_container_width=True,
                    hide_index=True,
                )
            if orphan_sells:
                st.markdown("**Sell (unlinked)**")
                df_orphan_sells = _project_columns(
                    to_dataframe(orphan_sells), SELL_DISPLAY_COLS
                )
                df_orphan_sells = _with_sell_mode_indicator(df_orphan_sells)
                st.dataframe(
                    df_orphan_sells,
                    use_container_width=True,
                    hide_index=True,
                )
        return

    for i, (ex, buys, sells) in enumerate(sessions_filtered):
        eid = ex.get("id", "?")
        title_bits = [
            str(ex.get("at_the_money_time", "—")),
            str(ex.get("exchange", "—")),
            str(ex.get("started_time", "—")),
        ]
        eid_short = eid if len(str(eid)) <= 14 else str(eid)[:12] + "…"
        label = f"Session {i + 1}: {' · '.join(title_bits)} | id {eid_short}"

        with st.expander(label, expanded=(i == 0)):
            summary = {k: ex.get(k, "—") for k in EXEC_SUMMARY_COLS}
            if ex.get("note"):
                summary["note"] = ex["note"]
            st.markdown("**Execution snapshot**")
            st.table(pd.DataFrame([summary]).T.rename(columns={0: "value"}))

            st.markdown("**Buy orders** (" + str(len(buys)) + ")")
            if buys:
                df_b = _project_columns(to_dataframe(buys), BUY_DISPLAY_COLS)
                st.dataframe(df_b, use_container_width=True, hide_index=True)
            else:
                st.caption("No linked buy rows.")

            st.markdown("**Sell orders** (" + str(len(sells)) + ")")
            if sells:
                df_s = _project_columns(to_dataframe(sells), SELL_DISPLAY_COLS)
                df_s = _with_sell_mode_indicator(df_s)
                st.dataframe(df_s, use_container_width=True, hide_index=True)
            else:
                st.caption("No linked sell rows.")

    if orphan_buys or orphan_sells:
        st.divider()
        st.subheader("Unlinked orders")
        st.caption("Rows with empty `dataForOrderExecutionId` (legacy runs or manual inserts).")
        if orphan_buys:
            st.markdown("**Buy (unlinked)**")
            st.dataframe(
                _project_columns(to_dataframe(orphan_buys), BUY_DISPLAY_COLS),
                use_container_width=True,
                hide_index=True,
            )
        if orphan_sells:
            st.markdown("**Sell (unlinked)**")
            df_orphan_sells = _project_columns(to_dataframe(orphan_sells), SELL_DISPLAY_COLS)
            df_orphan_sells = _with_sell_mode_indicator(df_orphan_sells)
            st.dataframe(
                df_orphan_sells,
                use_container_width=True,
                hide_index=True,
            )


if st.button("Refresh Data"):
    st.cache_data.clear()

sell_rows, buy_rows, exec_rows = load_table_data()

tab1, tab2, tab3, tab4 = st.tabs(
    [
        "sell_orderdetails",
        "BuyOrderDetails",
        "DataForOrderExecution",
        "Linked overview",
    ]
)

with tab1:
    st.subheader("sell_orderdetails (SellOrderDetails)")
    df_sell = to_dataframe(sell_rows)
    df_sell = _with_sell_mode_indicator(df_sell)
    st.write(f"Total rows: {len(df_sell)}")
    if df_sell.empty:
        st.info("No records found.")
    else:
        st.dataframe(df_sell, use_container_width=True, hide_index=True)

with tab2:
    st.subheader("BuyOrderDetails")
    df_buy = to_dataframe(buy_rows)
    st.write(f"Total rows: {len(df_buy)}")
    if df_buy.empty:
        st.info("No records found.")
    else:
        st.dataframe(df_buy, use_container_width=True, hide_index=True)

with tab3:
    st.subheader("DataForOrderExecution")
    df_exec = to_dataframe(exec_rows)
    st.write(f"Total rows: {len(df_exec)}")
    if df_exec.empty:
        st.info("No records found.")
    else:
        st.dataframe(df_exec, use_container_width=True, hide_index=True)

with tab4:
    render_linked_overview_tab(exec_rows, buy_rows, sell_rows)
