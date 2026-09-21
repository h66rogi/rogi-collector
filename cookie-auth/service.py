"""SOOP cookie acquisition for rogimarble; no platform calls in the test suite."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import hmac
import json
import math
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse, unquote
import urllib.request
import urllib.error

DAILY = 24 * 60 * 60
MAX_BYTES = 256 * 1024
LOGIN_URL = "https://login.sooplive.com/afreeca/login.php?szFrom=full&request_uri=https%3A%2F%2Fwww.sooplive.com%2F"
DOMAINS = {"sooplive.co.kr", "sooplive.com"}


class CookieError(Exception):
    """Only fixed, non-secret error codes cross the service boundary."""


def allowed_domain(domain):
    if not isinstance(domain, str):
        return False
    host = domain.lower().lstrip(".")
    return any(host == root or host.endswith("." + root) for root in DOMAINS)


def normalize_cookies(raw, now):
    if not isinstance(raw, list) or len(raw) > 300:
        raise CookieError("invalid_cookie_result")
    result = []
    for item in raw:
        if not isinstance(item, dict) or not allowed_domain(item.get("domain")):
            continue
        name, value = item.get("name"), item.get("value")
        path = item.get("path", "/")
        if (not isinstance(name, str) or not name or any(ord(c) < 33 or ord(c) > 126 for c in name) or any(c in name for c in '()<>@,;:\\"/[]?={} \t\r\n')
                or not isinstance(value, str) or not value or any(ord(c) < 33 or ord(c) > 126 or c in '\";,\\' for c in value)
                or not isinstance(path, str) or not path.startswith("/")):
            continue
        expires = item.get("expiry", item.get("expires", 0)) or 0
        if not isinstance(expires, (int, float)) or isinstance(expires, bool) or not math.isfinite(expires):
            continue
        expires = max(0, int(expires))  # CDP uses -1 for session cookies.
        if expires and expires <= now:
            continue
        result.append({"name": name, "value": value, "domain": item["domain"].lower(),
                       "path": path, "secure": bool(item.get("secure")),
                       "httpOnly": bool(item.get("httpOnly")), "expires": expires})
    if not result:
        raise CookieError("no_usable_cookies")
    return result


def authenticated_cookies(cookies, user_id):
    """A fresh browser must receive an auth ticket and matching account ticket.

    This verifies login evidence, not age verification or access to a live room.
    No ticket contents are fabricated or changed.
    """
    auth_domains = {c["domain"].lstrip(".") for c in cookies
                    if c["name"] == "AuthTicket" and c["value"]}
    return any(c["name"] == "UserTicket" and c["domain"].lstrip(".") in auth_domains
               and parse_qs(unquote(c["value"]) if "uid=" not in c["value"] else c["value"]).get("uid") == [user_id]
               for c in cookies)


def browser_login():
    # Read then remove credentials before Chrome/ChromeDriver inherit the environment.
    user_id, password = os.environ.pop("SOOP_ID", ""), os.environ.pop("SOOP_PW", "")
    if not user_id or not password:
        raise CookieError("credentials_missing")
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-dev-shm-usage")
    # No persisted browser profile, password store, screenshots or HTML dumps.
    if os.environ.get("SOOP_CHROME_BINARY"):
        options.binary_location = os.environ["SOOP_CHROME_BINARY"]
    driver_path = os.environ.get("SOOP_CHROMEDRIVER")
    if not driver_path:
        raise CookieError("chromedriver_missing")
    driver = None
    try:
        driver = webdriver.Chrome(service=Service(executable_path=driver_path, log_output=subprocess.DEVNULL), options=options)
        driver.set_page_load_timeout(25)
        driver.get(LOGIN_URL)
        login_location = urlparse(driver.current_url)
        if login_location.scheme != "https" or login_location.hostname not in {"login.sooplive.co.kr", "login.sooplive.com"}:
            raise CookieError("login_origin_rejected")
        wait = WebDriverWait(driver, 25)
        wait.until(EC.visibility_of_element_located((By.ID, "uid"))).send_keys(user_id)
        driver.find_element(By.ID, "password").send_keys(password)
        wait.until(EC.element_to_be_clickable((By.ID, "login_btn"))).click()

        def completed(browser):
            location = urlparse(browser.current_url)
            if location.scheme != "https" or not allowed_domain(location.hostname) or location.hostname.startswith("login."):
                return False
            try:
                cookies = normalize_cookies(browser.execute_cdp_cmd("Storage.getCookies", {})["cookies"], time.time())
                return cookies if authenticated_cookies(cookies, user_id) else False
            except CookieError:
                return False

        return wait.until(completed)
    finally:
        if driver is not None:
            driver.quit()


def acquire_in_child():
    # A hard deadline also covers a stuck browser driver. Kill its entire group.
    child_env = {key: os.environ[key] for key in ("PATH", "HOME", "LANG", "TMPDIR", "SOOP_ID", "SOOP_PW", "SOOP_CHROME_BINARY", "SOOP_CHROMEDRIVER") if key in os.environ}
    process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "login-child"], env=child_env,
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        output, _ = process.communicate(timeout=120)
        if process.returncode or len(output) > MAX_BYTES:
            raise CookieError("login_not_confirmed")
        return json.loads(output)
    except subprocess.TimeoutExpired:
        raise CookieError("login_timeout") from None
    except (ValueError, OSError):
        raise CookieError("login_not_confirmed") from None
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def atomic_store(path, snapshot):
    payload = json.dumps(snapshot, ensure_ascii=True, separators=(",", ":")).encode()
    if len(payload) > MAX_BYTES:
        raise CookieError("cookie_result_too_large")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix=".cookies-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        parent_fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        if os.path.exists(name):
            os.unlink(name)


class CookieManager:
    def __init__(self, path, account_id, login=acquire_in_child, clock=time.time):
        self.path, self.login, self.clock = Path(path), login, clock
        self.account_id = account_id
        self.account_key = hashlib.sha256(account_id.encode()).hexdigest()
        self.condition = threading.Condition()
        self.refreshing = False
        self.last_attempt = 0
        self.updated_at = 0
        self.error = None
        try:
            if self.path.stat().st_size > MAX_BYTES:
                raise ValueError()
            data = json.loads(self.path.read_bytes())
            if (data["version"] == 1 and data["account_key"] == self.account_key
                    and 0 < data["updated_at"] <= self.clock()
                    and authenticated_cookies(normalize_cookies(data["cookies"], self.clock()), account_id)):
                self.updated_at = data["updated_at"]
        except (OSError, ValueError, KeyError, TypeError, CookieError):
            pass
        if not self.updated_at and self.path.exists():
            self.path.replace(str(self.path) + ".rejected")

    def status(self):
        with self.condition:
            return {"state": "refreshing" if self.refreshing else ("refresh_failed" if self.error else ("ready" if self.updated_at else "not_ready")),
                    "updated_at": self.updated_at or None,
                    "next_refresh_at": self.updated_at + DAILY if self.updated_at else None,
                    "error": self.error}

    def due(self):
        with self.condition:
            return (not self.refreshing and self.clock() >= self.updated_at + DAILY
                    and (not self.last_attempt or self.clock() - self.last_attempt >= (DAILY if self.error in ("login_not_confirmed", "login_timeout") else 300)))

    def refresh(self):
        with self.condition:
            if self.refreshing:
                if not self.condition.wait_for(lambda: not self.refreshing, timeout=130):
                    raise CookieError("refresh_in_progress")
                if self.error:
                    raise CookieError(self.error)
                return self.status()
            # Coalesce adjacent requests too; avoid multiple logins invalidating each other.
            if self.last_attempt and self.clock() - self.last_attempt < 60:
                if self.error:
                    raise CookieError(self.error)
                return self.status()
            self.refreshing = True
            self.last_attempt = self.clock()
        error = None
        try:
            cookies = normalize_cookies(self.login(), self.clock())
            if not authenticated_cookies(cookies, self.account_id):
                raise CookieError("login_not_confirmed")
            now = int(self.clock())
            atomic_store(self.path, {"version": 1, "account_key": self.account_key, "updated_at": now, "cookies": cookies})
            with self.condition:
                self.updated_at = now
        except CookieError as exc:
            error = str(exc)
        except Exception:
            # Browser/OS exceptions can contain credentials, paths or page content.
            error = "refresh_failed"
        finally:
            with self.condition:
                self.error = error
                self.refreshing = False
                self.condition.notify_all()
        if error:
            raise CookieError(error)
        return self.status()


def make_handler(manager, token):
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(10)

        def log_message(self, *_args):
            pass

        def reply(self, code, data):
            body = json.dumps(data).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def authorized(self):
            candidate = self.headers.get("Authorization", "").encode()
            if not hmac.compare_digest(candidate, ("Bearer " + token).encode()):
                self.reply(401, {"error": "unauthorized"})
                return False
            return True

        def do_GET(self):
            if self.path == "/healthz":
                self.reply(200, {"state": "alive"})
            elif self.path == "/v1/status" and self.authorized():
                self.reply(200, manager.status())
            elif self.path != "/v1/status":
                self.reply(404, {"error": "not_found"})

        def do_POST(self):
            if not self.authorized():
                return
            if self.path != "/v1/refresh":
                self.reply(404, {"error": "not_found"})
                return
            if self.headers.get("Content-Length", "0") != "0" or self.headers.get("Transfer-Encoding"):
                self.reply(400, {"error": "body_not_allowed"})
                return
            try:
                self.reply(200, manager.refresh())
            except CookieError as exc:
                self.reply(503, {"error": str(exc)})
    return Handler


def serve():
    user_id, password = os.environ.get("SOOP_ID", ""), os.environ.get("SOOP_PW", "")
    token, path = os.environ.get("SOOP_COOKIE_API_TOKEN", ""), os.environ.get("SOOP_COOKIE_FILE", "")
    if not user_id or not password or len(token) < 32 or not path or not Path(path).is_absolute():
        raise CookieError("set_SOOP_ID_SOOP_PW_absolute_COOKIE_FILE_and_API_TOKEN_min32")
    cookie_path = Path(path)
    cookie_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # One writer per cookie volume, also across service restarts/duplicate processes.
    with open(str(cookie_path) + ".lock", "a") as lock:
        os.chmod(lock.name, 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CookieError("cookie_writer_already_running") from None
        manager = CookieManager(cookie_path, user_id)
        server = ThreadingHTTPServer((os.environ.get("SOOP_COOKIE_BIND", "127.0.0.1"),
                                      int(os.environ.get("SOOP_COOKIE_PORT", "8091"))), make_handler(manager, token))
        server.daemon_threads = False  # Keep the writer lock until an in-flight refresh finishes.
        stop = threading.Event()
        def shutdown(_signum, _frame):
            stop.set()
            threading.Thread(target=server.shutdown, daemon=True).start()
        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)
        def schedule():
            while not stop.is_set():
                if manager.due():
                    try:
                        manager.refresh()
                    except CookieError:
                        pass  # Status exposes a fixed error code; preserve prior cookies.
                stop.wait(10)
        scheduler = threading.Thread(target=schedule)
        scheduler.start()
        try:
            server.serve_forever(poll_interval=0.25)
        finally:
            stop.set()
            scheduler.join()
            server.server_close()


def request_refresh():
    token = os.environ.get("SOOP_COOKIE_API_TOKEN", "")
    base = os.environ.get("SOOP_COOKIE_API_URL", "http://127.0.0.1:8091")
    url = urlparse(base)
    if len(token) < 32 or url.scheme not in ("http", "https") or not url.hostname or url.username or url.query or url.fragment or url.path not in ("", "/"):
        raise CookieError("refresh_client_configuration_invalid")
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *_args):
            return None
    opener = urllib.request.build_opener(NoRedirect())
    request = urllib.request.Request(base.rstrip("/") + "/v1/refresh", data=b"", method="POST",
                                     headers={"Authorization": "Bearer " + token})
    try:
        with opener.open(request, timeout=135) as response:
            result = json.loads(response.read(4096))
    except Exception:
        raise CookieError("refresh_request_failed") from None
    # Whitelist metadata rather than printing an arbitrary response body.
    print(json.dumps({key: result.get(key) for key in ("state", "updated_at", "next_refresh_at", "error")}))


def load_role_environment():
    path = os.environ.get("ROLE_ENV_FILE")
    if not path:
        return
    try:
        for line in Path(path).read_text().splitlines():
            if not line or line.startswith("#"):
                continue
            name, separator, value = line.partition("=")
            if not separator or not re.fullmatch(r"[A-Z][A-Z0-9_]*", name):
                raise ValueError()
            os.environ.setdefault(name, value)
    except (OSError, ValueError):
        raise CookieError("role_environment_unavailable") from None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["serve", "refresh", "login-child"], default="serve", nargs="?")
    args = parser.parse_args()
    try:
        load_role_environment()
        if args.mode == "login-child":
            # Only the parent reads this pipe. Not a user-facing cookie export CLI.
            if sys.stdout.isatty():
                raise CookieError("internal_mode_requires_pipe")
            print(json.dumps(browser_login()))
        elif args.mode == "refresh":
            request_refresh()
        else:
            serve()
    except Exception:
        print("cookie component failed; check configuration or authenticated status", file=sys.stderr)
        sys.exit(1)
