"""Streamlit page: insert broker credentials into Postgres via db.save_user_credentials."""

from __future__ import annotations

import asyncio

import streamlit as st

from db import encrypt_text, save_user_credentials

st.set_page_config(page_title="Save credentials", layout="centered")
st.title("Save credentials")
st.caption(
    "Inserts a new row into the `Credentials` table. Each submit generates a new `id`."
)

if st.button("← Back to Trading DB Visualization"):
    st.switch_page("db_visulisation.py")

with st.form("credentials_form"):
    user = st.text_input("User / account name", autocomplete="off")
    api_key = st.text_input("API key", autocomplete="off")
    api_secrets = st.text_input("API secret", type="password", autocomplete="off")
    phone_no = st.text_input("Phone number", autocomplete="off")
    totp_bar_code = st.text_input("TOTP secret", type="password", autocomplete="off")
    pin_code = st.text_input("PIN", type="password", autocomplete="off")
    submitted = st.form_submit_button("Save to database")

if submitted:
    try:
        row = asyncio.run(
            save_user_credentials(
                user.strip(),
                encrypt_text(api_key.strip()),
                encrypt_text(api_secrets.strip()),
                encrypt_text(phone_no.strip()),
                encrypt_text(totp_bar_code.strip()),
                encrypt_text(pin_code.strip()),
            )
        )
        if row:
            st.success("Saved successfully.")
            st.json(row)
        else:
            st.warning("Save completed but no row was returned.")
    except ValueError as exc:
        st.error(str(exc))
    except Exception as exc:
        st.exception(exc)
