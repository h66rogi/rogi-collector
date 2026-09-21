import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock
import urllib.error
import urllib.request

spec = importlib.util.spec_from_file_location("cookie_service", Path(__file__).with_name("service.py"))
service = importlib.util.module_from_spec(spec)
spec.loader.exec_module(service)
NOW = 1_800_000_000


def cookies(value="synthetic-auth", user="fixture-viewer"):
    return [{"name": "AuthTicket", "value": value, "domain": ".sooplive.co.kr", "path": "/", "secure": True},
            {"name": "UserTicket", "value": "uid%3D" + user, "domain": ".sooplive.co.kr", "path": "/", "secure": True}]


class CookieTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "cookies.json"
        self.now = NOW
        self.clock = lambda: self.now

    def manager(self, login=lambda: cookies()):
        return service.CookieManager(self.path, "fixture-viewer", login, self.clock)

    def test_startup_daily_and_manual_refresh_persist_across_restart(self):
        calls = []
        manager = self.manager(lambda: calls.append(1) or cookies(str(len(calls))))
        self.assertTrue(manager.due())
        manager.refresh()
        self.assertFalse(manager.due())
        self.assertEqual(0o600, self.path.stat().st_mode & 0o777)
        restarted = self.manager()
        self.assertFalse(restarted.due())
        self.now += service.DAILY
        self.assertTrue(restarted.due())
        manager.refresh()
        self.assertEqual(2, len(calls))
        self.now += 61
        manager.refresh()  # Explicit requests work before the next daily deadline.
        self.assertEqual(3, len(calls))
        self.assertEqual("3", json.loads(self.path.read_bytes())["cookies"][0]["value"])

    def test_failed_login_does_not_replace_previous_snapshot(self):
        manager = self.manager()
        manager.refresh()
        before = self.path.read_bytes()
        self.now += 61
        manager.login = lambda: cookies(user="wrong-fixture-user")
        with self.assertRaisesRegex(service.CookieError, "login_not_confirmed"):
            manager.refresh()
        self.assertEqual(before, self.path.read_bytes())
        self.assertEqual("refresh_failed", manager.status()["state"])
        self.assertEqual(NOW, manager.status()["updated_at"])

    def test_browser_and_disk_failures_are_redacted_and_leave_previous_file(self):
        manager = self.manager()
        manager.refresh()
        before = self.path.read_bytes()
        self.now += 61
        manager.login = mock.Mock(side_effect=RuntimeError("synthetic-private-content"))
        with self.assertRaisesRegex(service.CookieError, "^refresh_failed$"):
            manager.refresh()
        self.assertNotIn("synthetic-private-content", json.dumps(manager.status()))
        self.now += 61
        manager.login = lambda: cookies("new")
        with mock.patch.object(service.os, "replace", side_effect=OSError("disk full")):
            with self.assertRaises(service.CookieError):
                manager.refresh()
        self.assertEqual(before, self.path.read_bytes())
        self.assertEqual([], list(self.path.parent.glob(".cookies-*")))

    def test_parallel_refreshes_login_once(self):
        entered, release = threading.Event(), threading.Event()
        calls = []
        def login():
            calls.append(1)
            entered.set()
            self.assertTrue(release.wait(3))
            return cookies()
        manager = self.manager(login)
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            first = pool.submit(manager.refresh)
            self.assertTrue(entered.wait(2))
            others = [pool.submit(manager.refresh) for _ in range(5)]
            release.set()
            statuses = [first.result()] + [future.result() for future in others]
        self.assertEqual(1, len(calls))
        self.assertTrue(all(s["updated_at"] == NOW for s in statuses))

    def test_cookie_domains_expiry_and_header_injection(self):
        raw = cookies() + [
            {"name": "bad", "value": "x", "domain": ".sooplive.co.kr.evil.invalid"},
            {"name": "old", "value": "x", "domain": ".sooplive.co.kr", "expiry": NOW - 1},
            {"name": "inject", "value": "x\r\nCookie: x", "domain": ".sooplive.co.kr"},
            {"name": "nan", "value": "x", "domain": ".sooplive.co.kr", "expiry": float('nan')},
        ]
        normalized = service.normalize_cookies(raw, NOW)
        self.assertEqual(2, len(normalized))
        self.assertTrue(service.authenticated_cookies(normalized, "fixture-viewer"))
        normalized[0]["name"] = "analytics"
        self.assertFalse(service.authenticated_cookies(normalized, "fixture-viewer"))

    def test_http_refresh_requires_token_and_never_returns_cookies(self):
        manager = self.manager()
        token = "synthetic-internal-token-for-tests"
        server = service.ThreadingHTTPServer(("127.0.0.1", 0), service.make_handler(manager, token))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_port}"
        request = urllib.request.Request(url + "/v1/refresh", method="POST")
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(request)
        self.assertEqual(401, error.exception.code)
        self.assertFalse(self.path.exists())
        request.add_header("Authorization", "Bearer " + token)
        with urllib.request.urlopen(request) as response:
            body = response.read().decode()
            self.assertEqual("no-store", response.headers["Cache-Control"])
        self.assertEqual("ready", json.loads(body)["state"])
        self.assertNotIn("synthetic-auth", body)
        self.assertNotIn("fixture-viewer", body)
        for suffix in ["/v1/cookies", "/cookies.json"]:
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(url + suffix)
            self.assertEqual(404, error.exception.code)

    def test_changed_account_does_not_reuse_previous_account_cookies(self):
        self.manager().refresh()
        manager = service.CookieManager(self.path, "different-fixture-user", clock=self.clock)
        self.assertFalse(self.path.exists())
        self.assertTrue(Path(str(self.path) + ".rejected").exists())
        self.assertEqual("not_ready", manager.status()["state"])

    def test_failed_auth_does_not_retry_login_every_poll(self):
        manager = self.manager(lambda: cookies(user="wrong-fixture-user"))
        with self.assertRaises(service.CookieError):
            manager.refresh()
        self.now += 301
        self.assertFalse(manager.due())
        self.now += service.DAILY
        self.assertTrue(manager.due())

    def test_browser_login_checks_fresh_matching_cookies_before_return(self):
        driver = mock.Mock()
        driver.current_url = service.LOGIN_URL
        driver.execute_cdp_cmd.return_value = {"cookies": cookies()}
        field, button = mock.Mock(), mock.Mock()
        def clicked():
            driver.current_url = "https://www.sooplive.co.kr/"
        button.click.side_effect = clicked
        wait = mock.Mock()
        count = 0
        def until(condition):
            nonlocal count
            count += 1
            return field if count == 1 else button if count == 2 else condition(driver)
        wait.until.side_effect = until
        with mock.patch.dict(os.environ, {"SOOP_ID": "fixture-viewer", "SOOP_PW": "fixture-password", "SOOP_CHROMEDRIVER": "/fake/driver"}), \
             mock.patch("selenium.webdriver.Chrome", return_value=driver), \
             mock.patch("selenium.webdriver.support.ui.WebDriverWait", return_value=wait):
            result = service.browser_login()
        self.assertTrue(service.authenticated_cookies(result, "fixture-viewer"))
        field.send_keys.assert_called_once_with("fixture-viewer")
        driver.quit.assert_called_once()

    def test_browser_does_not_type_credentials_after_untrusted_redirect(self):
        driver = mock.Mock()
        driver.current_url = "https://example.invalid/credential-capture"
        with mock.patch.dict(os.environ, {"SOOP_ID": "fixture-viewer", "SOOP_PW": "fixture-password", "SOOP_CHROMEDRIVER": "/fake/driver"}), \
             mock.patch("selenium.webdriver.Chrome", return_value=driver):
            with self.assertRaisesRegex(service.CookieError, "login_origin_rejected"):
                service.browser_login()
        driver.find_element.assert_not_called()
        driver.quit.assert_called_once()

    def test_browser_is_stopped_even_when_login_fails(self):
        # Tests the actual browser adapter orchestration using a fake driver,
        # without opening a browser, site or supplying real credentials.
        from selenium.common.exceptions import TimeoutException
        driver = mock.Mock()
        driver.current_url = service.LOGIN_URL
        wait = mock.Mock()
        wait.until.side_effect = [mock.Mock(), mock.Mock(), TimeoutException()]
        with mock.patch.dict(os.environ, {"SOOP_ID": "fixture-viewer", "SOOP_PW": "fixture-password", "SOOP_CHROMEDRIVER": "/fake/driver"}), \
             mock.patch("selenium.webdriver.Chrome", return_value=driver), \
             mock.patch("selenium.webdriver.support.ui.WebDriverWait", return_value=wait):
            with self.assertRaises(TimeoutException):
                service.browser_login()
            self.assertNotIn("SOOP_ID", os.environ)
            self.assertNotIn("SOOP_PW", os.environ)
        driver.quit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
