"""
Debug script — traces the UK Ratings redirect chain step by step.

Run from the project root:
    python scripts/debug_ukratings_login.py

This does NOT actually log in — it just shows what the server returns at
each step so we can understand why programmatic login is failing.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from dotenv import load_dotenv
load_dotenv()

BASE = "https://www.ukratings.co.uk"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


def trace(session, url, method="GET", **kwargs):
    """Make one request without following redirects and print what happens."""
    resp = session.request(method, url, allow_redirects=False, **kwargs)
    location = resp.headers.get("Location", "—")
    cookies_set = {k: v[:20] + "..." for k, v in resp.cookies.items()}
    print(f"  {method} {url}")
    print(f"    Status   : {resp.status_code}")
    print(f"    Location : {location}")
    print(f"    Cookies  : {cookies_set or '(none)'}")
    print(f"    Session cookies so far: {dict(session.cookies)}")
    print()
    return resp


def main():
    username = os.getenv("UK_RATINGS_USERNAME", "")
    password = os.getenv("UK_RATINGS_PASSWORD", "")
    print(f"Credentials loaded: username={username!r}, password={'***' if password else '(empty)'}\n")

    session = requests.Session()
    session.headers.update(HEADERS)

    print("=== Step 1: GET homepage ===")
    r1 = trace(session, f"{BASE}/")

    print("=== Step 2: GET /login/ ===")
    r2 = trace(session, f"{BASE}/login/")

    if r2.status_code == 200:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r2.text, "lxml")
        csrf = soup.find("input", {"name": "csrfmiddlewaretoken"})
        print(f"  Login form found! CSRF token: {csrf.get('value', '')[:20] if csrf else 'NOT FOUND'}")
        print()

        if csrf:
            print("=== Step 3: POST credentials ===")
            r3 = trace(session, f"{BASE}/login/",
                method="POST",
                data={
                    "useremail": username,
                    "password": password,
                    "csrfmiddlewaretoken": csrf.get("value", ""),
                    "next": "/",
                },
                headers={"Referer": f"{BASE}/login/"},
            )
            if r3.status_code in (301, 302):
                loc = r3.headers.get("Location", "")
                print(f"=== Step 4: Follow POST redirect → {loc} ===")
                trace(session, loc if loc.startswith("http") else f"{BASE}{loc}")
    else:
        print(f"  Login page returned {r2.status_code}, not 200 — cannot get CSRF token")
        if r2.status_code in (301, 302):
            loc = r2.headers.get("Location", "—")
            print(f"  Redirecting to: {loc}")
            print(f"  Following...")
            trace(session, loc if loc.startswith("http") else f"{BASE}{loc}")


if __name__ == "__main__":
    main()
