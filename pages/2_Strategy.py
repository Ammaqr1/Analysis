"""Streamlit page: manage Strategy rows."""

from __future__ import annotations

import asyncio
import importlib

import pandas as pd
import streamlit as st

import db as analysis_db

analysis_db = importlib.reload(analysis_db)
create_strategy = analysis_db.create_strategy
delete_strategy = analysis_db.delete_strategy
get_strategy_rows_sync = analysis_db.get_strategy_rows_sync
get_strategy_names_sync = analysis_db.get_strategy_names_sync
save_strategy_name = analysis_db.save_strategy_name
update_strategy = analysis_db.update_strategy

STRATEGY_FIELDS = [
    "strategy_name",
    "week_day",
    "at_time_money",
    "start_time",
    "end_time",
    "end_entry_time",
    "exchange",
    "target_price",
    "stop_loss_price",
    "target_trigger_price_percentage",
    "stoploss_trigger_price_percentage",
]

WEEKDAY_OPTIONS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


def _rerun() -> None:
    rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
    if rerun:
        rerun()


def _field_label(field: str) -> str:
    return field.replace("_", " ").title()


def _weekday_index(value: object) -> int:
    value_text = str(value or "").strip().lower()
    for idx, day in enumerate(WEEKDAY_OPTIONS):
        if day.lower() == value_text:
            return idx
    return 0


def _render_strategy_fields(prefix: str, defaults: dict | None = None) -> dict:
    defaults = defaults or {}
    values: dict[str, str] = {}
    left, right = st.columns(2)
    for idx, field in enumerate(STRATEGY_FIELDS):
        container = left if idx % 2 == 0 else right
        if field == "week_day":
            values[field] = container.selectbox(
                _field_label(field),
                WEEKDAY_OPTIONS,
                index=_weekday_index(defaults.get(field)),
                key=f"{prefix}_{field}",
            )
        else:
            values[field] = container.text_input(
                _field_label(field),
                value=str(defaults.get(field) or ""),
                key=f"{prefix}_{field}",
                autocomplete="off",
            )
    return values


def _render_strategy_row_fields(
    prefix: str,
    strategy_name: str,
    defaults: dict | None = None,
) -> dict:
    defaults = defaults or {}
    values: dict[str, str] = {"strategy_name": strategy_name}
    st.text_input(
        _field_label("strategy_name"),
        value=strategy_name,
        disabled=True,
        key=f"{prefix}_strategy_name",
    )
    left, right = st.columns(2)
    for idx, field in enumerate(STRATEGY_FIELDS[1:]):
        container = left if idx % 2 == 0 else right
        if field == "week_day":
            values[field] = container.selectbox(
                _field_label(field),
                WEEKDAY_OPTIONS,
                index=_weekday_index(defaults.get(field)),
                key=f"{prefix}_{field}",
            )
        else:
            values[field] = container.text_input(
                _field_label(field),
                value=str(defaults.get(field) or ""),
                key=f"{prefix}_{field}",
                autocomplete="off",
            )
    return values


def _is_strategy_data_row(row: dict) -> bool:
    return any(str(row.get(field) or "").strip() for field in STRATEGY_FIELDS[1:])

st.set_page_config(page_title="Save strategy", layout="wide")
st.title("Strategy manager")
st.caption("First create a strategy name, then open it to manage rows under that name.")

if st.button("← Back to Trading DB Visualization"):
    st.switch_page("db_visulisation.py")

strategy_names = get_strategy_names_sync()
if "selected_strategy_name" not in st.session_state:
    st.session_state.selected_strategy_name = None

selected_name = st.session_state.selected_strategy_name

if selected_name is None:
    with st.form("strategy_name_form"):
        strategy_name = st.text_input("Create strategy name", autocomplete="off")
        submitted_name = st.form_submit_button("Create strategy name")

    if submitted_name:
        try:
            row = asyncio.run(save_strategy_name(strategy_name))
            if row:
                st.success("Strategy name created.")
                st.cache_data.clear()
                st.session_state.selected_strategy_name = strategy_name.strip()
                _rerun()
            else:
                st.warning("Save completed but no row was returned.")
        except ValueError as exc:
            st.error(str(exc))
        except Exception as exc:
            st.exception(exc)

    st.divider()
    st.subheader("Strategy names")
    if not strategy_names:
        st.info("No strategy names found.")
    else:
        for idx, name in enumerate(strategy_names, start=1):
            label = f"{idx}. {name}"
            if st.button(label, key=f"strategy_name_{idx}_{name}"):
                st.session_state.selected_strategy_name = name
                _rerun()
    st.stop()

if st.button("← Back to strategy names"):
    st.session_state.selected_strategy_name = None
    _rerun()

filtered_rows = get_strategy_rows_sync(selected_name)
data_rows = [row for row in filtered_rows if _is_strategy_data_row(row)]

st.subheader(f"Strategy: {selected_name}")
st.write(f"Total rows under this strategy: {len(data_rows)}")

if data_rows:
    st.dataframe(pd.DataFrame(data_rows), use_container_width=True, hide_index=True)
else:
    st.info("No rows found for this strategy name. Add the first row below.")

add_tab, edit_tab, delete_tab = st.tabs(["Add new row", "Update row", "Delete row"])

with add_tab:
    st.caption("Add a new record under this strategy name.")
    with st.form("add_strategy_form"):
        add_values = _render_strategy_row_fields("add_strategy", selected_name)
        submitted = st.form_submit_button("Add strategy row")

    if submitted:
        try:
            row = asyncio.run(create_strategy(add_values))
            st.success("Strategy row added.")
            st.json(row)
            st.cache_data.clear()
            _rerun()
        except ValueError as exc:
            st.error(str(exc))
        except Exception as exc:
            st.exception(exc)

with edit_tab:
    if not data_rows:
        st.info("Add a strategy row before updating.")
    else:
        row_options = {
            f"{row.get('week_day') or 'No weekday'} | {row.get('id')}": row
            for row in data_rows
        }
        selected_label = st.selectbox(
            "Choose row to update",
            list(row_options),
            key="update_strategy_row",
        )
        selected_row = row_options[selected_label]
        with st.form("update_strategy_form"):
            update_values = _render_strategy_row_fields(
                f"update_strategy_{selected_row['id']}",
                selected_name,
                selected_row,
            )
            submitted_update = st.form_submit_button("Update strategy row")

        if submitted_update:
            try:
                row = asyncio.run(update_strategy(selected_row["id"], update_values))
                if row:
                    st.success("Strategy row updated.")
                    st.json(row)
                    st.cache_data.clear()
                    _rerun()
                else:
                    st.warning("No row was updated.")
            except ValueError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.exception(exc)

with delete_tab:
    if not data_rows:
        st.info("Add a strategy row before deleting.")
    else:
        row_options = {
            f"{row.get('week_day') or 'No weekday'} | {row.get('id')}": row
            for row in data_rows
        }
        selected_label = st.selectbox(
            "Choose row to delete",
            list(row_options),
            key="delete_strategy_row",
        )
        selected_row = row_options[selected_label]
        st.warning("This will permanently delete the selected Strategy row.")
        if st.button("Delete selected strategy row", type="primary"):
            try:
                row = asyncio.run(delete_strategy(selected_row["id"]))
                if row:
                    st.success("Strategy row deleted.")
                    st.cache_data.clear()
                    _rerun()
                else:
                    st.warning("No row was deleted.")
            except ValueError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.exception(exc)
