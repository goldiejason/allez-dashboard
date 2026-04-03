#!/usr/bin/env python3
"""
One-time helper: reads Supabase API keys from clipboard and writes them to .env.
Run from the allez-dashboard folder:
    python3 setup_keys.py
"""

import subprocess
import sys
from pathlib import Path

ENV_PATH = Path(__file__).parent / ".env"


def get_clipboard() -> str:
    """Read clipboard content cross-platform."""
    if sys.platform == "darwin":
        # macOS
        result = subprocess.run(["pbpaste"], capture_output=True, text=True)
        return result.stdout.strip()
    elif sys.platform.startswith("linux"):
        # Linux — try xclip then xsel, fall back to manual input
        for cmd in [
            ["xclip", "-selection", "clipboard", "-o"],
            ["xsel", "--clipboard", "--output"],
        ]:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    return result.stdout.strip()
            except FileNotFoundError:
                continue
        return input("  (clipboard tool not found) Paste the value and press ENTER: ").strip()
    else:
        # Windows
        result = subprocess.run(
            ["powershell", "-NoProfile", "-command", "Get-Clipboard"],
            capture_output=True, text=True,
        )
        return result.stdout.strip()


def update_env(key_name: str, value: str):
    content = ENV_PATH.read_text()
    if key_name not in content:
        print(f"  ⚠️  '{key_name}' not found in .env — skipping.")
        return
    lines = content.splitlines()
    new_lines = []
    for line in lines:
        if line.startswith(f"{key_name}="):
            new_lines.append(f"{key_name}={value}")
        else:
            new_lines.append(line)
    ENV_PATH.write_text("\n".join(new_lines) + "\n")
    print(f"  ✅  {key_name} written ({len(value)} chars)")


def main():
    print("\n🔑  Allez Dashboard — API Key Setup")
    print("=" * 45)
    print(f"  Will update: {ENV_PATH}\n")

    # ── Anon key ──────────────────────────────────
    print("STEP 1: Copy the anon key")
    print("  → In Chrome, go to the Supabase Legacy API Keys page")
    print("  → Click the 'Copy' button next to the anon / public key")
    input("  → Press ENTER here once you've copied it...")
    anon = get_clipboard()
    if not anon.startswith("eyJ"):
        print(f"  ⚠️  Clipboard doesn't look like a JWT (got: {anon[:30]!r})")
        print("      Make sure you clicked 'Copy' on the Supabase page first.")
        sys.exit(1)
    update_env("SUPABASE_ANON_KEY", anon)

    # ── Service role key ──────────────────────────
    print("\nSTEP 2: Copy the service_role key")
    print("  → Click 'Reveal' next to the service_role key")
    print("  → Then click 'Copy'")
    input("  → Press ENTER here once you've copied it...")
    sr = get_clipboard()
    if not sr.startswith("eyJ"):
        print(f"  ⚠️  Clipboard doesn't look like a JWT (got: {sr[:30]!r})")
        sys.exit(1)
    update_env("SUPABASE_SERVICE_ROLE_KEY", sr)

    print("\n✅  Done! Your .env is ready.")
    print("    You can now run:  streamlit run app.py")


if __name__ == "__main__":
    main()
