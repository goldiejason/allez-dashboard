"""
Supabase client — shared across all modules.
Uses service_role key for writes (collection scripts),
anon key for reads (dashboard / Streamlit).
"""
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

_SUPABASE_URL = os.environ["SUPABASE_URL"]
_ANON_KEY     = os.environ["SUPABASE_ANON_KEY"]
_SERVICE_KEY  = os.environ["SUPABASE_SERVICE_ROLE_KEY"]


def get_read_client() -> Client:
    """Read-only client using the anon/publishable key. Safe for dashboard."""
    return create_client(_SUPABASE_URL, _ANON_KEY)


def get_write_client() -> Client:
    """Write client using the service_role key. For collection scripts only."""
    return create_client(_SUPABASE_URL, _SERVICE_KEY)
