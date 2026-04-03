"""
Supabase client — shared across all modules.
Uses service_role key for writes (collection scripts),
anon key for reads (dashboard / Streamlit).

Env vars are read inside the factory functions rather than at module import
time so that scripts calling load_dotenv() before importing this module do
not raise a KeyError.  Streamlit Cloud injects vars natively; local dev
relies on dotenv being loaded first by the calling script.

Clients are cached as module-level singletons — each factory function
creates one instance on first call and reuses it thereafter.  This avoids
creating a new TCP connection on every dashboard render or calculator call.
"""
import os
from typing import Optional
from supabase import create_client, Client
from dotenv import load_dotenv

_read_client:  Optional[Client] = None
_write_client: Optional[Client] = None


def get_read_client() -> Client:
    """Read-only client using the anon/publishable key. Safe for dashboard."""
    global _read_client
    if _read_client is None:
        load_dotenv()
        _read_client = create_client(
            os.environ["SUPABASE_URL"],
            os.environ["SUPABASE_ANON_KEY"],
        )
    return _read_client


def get_write_client() -> Client:
    """Write client using the service_role key. For collection scripts only."""
    global _write_client
    if _write_client is None:
        load_dotenv()
        _write_client = create_client(
            os.environ["SUPABASE_URL"],
            os.environ["SUPABASE_SERVICE_ROLE_KEY"],
        )
    return _write_client
