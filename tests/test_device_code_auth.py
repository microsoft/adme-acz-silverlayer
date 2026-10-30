"""Offline notebook authentication tests with controlled time and MSAL boundaries."""

import ast
import contextlib
import io
import logging
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from unittest import mock

from test_notebook_simplification import extract_functions, load_notebook


class FakeClock:
    def __init__(self):
        self.now = 1000

    def __call__(self):
        return self.now


class FakePublicClient:
    def __init__(self):
        self.accounts = []
        self.account = {"local_account_id": "test-account", "realm": "test-tenant"}
        self.silent_result = {"access_token": "renewed-access", "expires_on": 3000}
        self.device_result = {
            "access_token": "device-access", "expires_on": 1800,
            "id_token_claims": {"oid": "test-account", "tid": "test-tenant"},
        }
        self.silent_calls = []
        self.device_calls = 0

    def get_accounts(self):
        return self.accounts

    def acquire_token_silent_with_error(self, *, scopes, account):
        self.silent_calls.append((scopes, account))
        return self.silent_result

    def initiate_device_flow(self, *, scopes):
        self.device_calls += 1
        return {"message": "Synthetic interactive sign-in"}

    def acquire_token_by_device_flow(self, flow):
        self.accounts = [self.account]
        return self.device_result


class DeviceCodeAuthTests(unittest.TestCase):
    def setUp(self):
        self.notebook = load_notebook()
        functions = extract_functions(self.notebook, [
            "_adme_schema_config", "_adme_auth_method", "_adme_auth_value", "_adme_authority_url",
            "_adme_managed_identity_client_id", "_msal_token_result", "_acquire_adme_access_token",
            "_acquire_adme_device_code_token", "_adme_token_cache_key", "get_adme_access_token",
        ])
        self.namespace = functions["get_adme_access_token"].__globals__
        self.clock = FakeClock()
        self.clients = []

        def factory(**kwargs):
            client = FakePublicClient()
            self.clients.append(client)
            return client

        self.factory = mock.Mock(side_effect=factory)
        self.namespace.update(
            adme_auth_method="DC", time=self.clock, RLock=RLock,
            logger=logging.getLogger("test_device_code_auth"), PublicClientApplication=self.factory,
        )
        self.initialize_state()
        self.get_token = functions["get_adme_access_token"]
        self.output = io.StringIO()
        self.redirect = contextlib.redirect_stdout(self.output)
        self.redirect.__enter__()
        self.addCleanup(self.redirect.__exit__, None, None, None)

    def initialize_state(self):
        for cell in self.notebook["cells"]:
            if cell["cell_type"] != "code":
                continue
            for node in ast.parse("".join(cell["source"])).body:
                if isinstance(node, ast.If) and ast.unparse(node.test) == "'_ADME_AUTH_STATE' not in globals()":
                    exec(compile(ast.Module(body=[node], type_ignores=[]), "<auth-state>", "exec"), self.namespace)
                    return
        self.fail("Notebook session authentication initializer is missing")

    def test_valid_token_and_helper_reruns_reuse_the_signed_in_session(self):
        self.assertEqual(self.get_token(), "device-access")
        state = self.namespace["_ADME_AUTH_STATE"]
        self.initialize_state()
        self.assertIs(self.namespace["_ADME_AUTH_STATE"], state)
        self.assertEqual(self.get_token(), "device-access")
        self.assertEqual(self.factory.call_count, 1)
        self.assertEqual(self.clients[0].device_calls, 1)
        self.assertEqual(self.clients[0].silent_calls, [])
        self.assertEqual(self.output.getvalue().count("Synthetic interactive sign-in"), 1)

    def test_expiring_token_renews_silently_using_the_same_client_and_account(self):
        self.get_token()
        self.clock.now = 1500
        self.assertEqual(self.get_token(), "renewed-access")
        self.assertEqual(self.factory.call_count, 1)
        client = self.clients[0]
        self.assertEqual(client.device_calls, 1)
        self.assertEqual(client.silent_calls, [([self.namespace["ADME_TOKEN_SCOPE"]], client.account)])
        self.assertEqual(self.get_token(), "renewed-access")
        self.assertEqual(len(client.silent_calls), 1)

    def test_existing_msal_account_is_used_before_starting_device_flow(self):
        client = FakePublicClient()
        client.accounts = [client.account]
        self.factory.side_effect = None
        self.factory.return_value = client
        self.assertEqual(self.get_token(), "renewed-access")
        self.assertEqual(client.device_calls, 0)
        self.assertEqual(self.output.getvalue(), "")

    def test_scope_tenant_client_and_method_changes_cannot_reuse_wrong_tokens(self):
        self.get_token()
        self.namespace["ADME_TOKEN_SCOPE"] = "https://resource.example/.default"
        self.assertEqual(self.get_token(), "renewed-access")
        self.assertEqual(self.factory.call_count, 1)
        self.assertEqual(self.clients[0].silent_calls[0][0], ["https://resource.example/.default"])
        self.namespace["adme_tenant_id"] = "another-tenant"
        self.assertEqual(self.get_token(), "device-access")
        self.namespace["ADME_DEVICE_CODE_CLIENT_ID"] = "another-client"
        self.assertEqual(self.get_token(), "device-access")
        self.assertEqual(self.factory.call_count, 3)
        self.namespace["adme_auth_method"] = "SP"
        acquire = mock.Mock(return_value=("service-access", 3000))
        self.namespace["_acquire_adme_access_token"] = acquire
        self.assertEqual(self.get_token(), "service-access")
        acquire.assert_called_once()

    def test_interaction_required_prompts_but_transient_errors_do_not(self):
        self.get_token()
        self.clock.now = 1500
        client = self.clients[0]
        client.silent_result = {"error": "temporarily_unavailable", "error_description": "Synthetic service outage"}
        cached = dict(self.namespace["_ADME_AUTH_STATE"]["tokens"])
        with self.assertRaisesRegex(RuntimeError, "silent device code authentication"):
            self.get_token()
        self.assertEqual(self.namespace["_ADME_AUTH_STATE"]["tokens"], cached)
        self.assertEqual(client.device_calls, 1)
        client.silent_result = {"error": "interaction_required"}
        with self.assertLogs("test_device_code_auth", level="WARNING"):
            self.assertEqual(self.get_token(), "device-access")
        self.assertEqual(client.device_calls, 2)

    def test_missing_silent_token_falls_back_to_device_flow(self):
        self.get_token()
        self.clock.now = 1500
        self.clients[0].silent_result = None
        self.assertEqual(self.get_token(), "device-access")
        self.assertEqual(self.clients[0].device_calls, 2)

    def test_concurrent_callers_start_only_one_device_flow(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(list(pool.map(lambda _: self.get_token(), range(8))), ["device-access"] * 8)
        self.assertEqual(self.factory.call_count, 1)
        self.assertEqual(self.clients[0].device_calls, 1)

    def test_failed_client_initialization_or_login_does_not_publish_tokens(self):
        self.factory.side_effect = RuntimeError("Synthetic initialization failure")
        with self.assertRaisesRegex(RuntimeError, "initialization"):
            self.get_token()
        self.assertEqual(self.namespace["_ADME_AUTH_STATE"]["device_clients"], {})
        self.assertEqual(self.namespace["_ADME_AUTH_STATE"]["tokens"], {})
        client = FakePublicClient()
        client.device_result = {"error": "access_denied"}
        self.factory.side_effect = None
        self.factory.return_value = client
        with self.assertRaisesRegex(RuntimeError, "access_denied"):
            self.get_token()
        self.assertEqual(self.namespace["_ADME_AUTH_STATE"]["tokens"], {})

    def test_ambiguous_preexisting_accounts_fail_without_choosing_a_user(self):
        client = FakePublicClient()
        client.accounts = [client.account, {"local_account_id": "another-account", "realm": "test-tenant"}]
        self.factory.side_effect = None
        self.factory.return_value = client
        with self.assertRaisesRegex(RuntimeError, "multiple accounts"):
            self.get_token()
        self.assertEqual(client.device_calls, 0)
        self.assertEqual(self.namespace["_ADME_AUTH_STATE"]["tokens"], {})

    def test_new_notebook_session_requires_a_new_sign_in(self):
        self.get_token()
        del self.namespace["_ADME_AUTH_STATE"]
        self.initialize_state()
        self.assertEqual(self.get_token(), "device-access")
        self.assertEqual(self.factory.call_count, 2)


if __name__ == "__main__":
    unittest.main()
