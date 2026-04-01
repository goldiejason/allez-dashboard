"""
Supabase client — shared across all modules.
Uses service_role key for writes (collection scripts),
anon key for reads (dashboard / Streamlit).

Env vars are read inside the factory functions rather than at module import
time so that scripts calling load_dotenv() before importing this module do
not raise a KeyError.  Streamlit Cloud injects vars natively; local dev
relies on dotenv being loaded first by the calling script.
"""
import os
from supabase import create_client, Client
from dotenv import load_dotenv


def get_read_client() -> Client:
    """Read-only client using the anon/publishable key. Safe for dashboard."""
    load_dotenv()
    return create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_ANON_KEY"],
    )


def get_write_client() -> Client:
    """Write client using the service_role key. For collection scripts only."""
    load_dotenv()
    return create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )
