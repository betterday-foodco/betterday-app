"""Unit tests for culinary_ops_client.

Run from repo root with:
    python -m unittest tests.test_culinary_ops_client -v

Uses stdlib unittest + unittest.mock so no new deps are added to
requirements.txt. The HTTP layer is injected via the `http=` constructor
arg, so no real network calls are ever made.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from culinary_ops_client import (
    CulinaryOpsClient,
    _is_enabled,
    get_culinary_client,
)


def _mock_http(status=200, body=None, raise_exc=None):
    """Build a fake `requests`-like object.

    Returns an object whose `.request(method, url, **kwargs)` returns a
    response with `.status_code` and `.json()`. If `raise_exc` is set,
    `.request()` raises it instead (transport-error simulation).
    """
    http = MagicMock()
    if raise_exc is not None:
        http.request.side_effect = raise_exc
        return http
    response = MagicMock()
    response.status_code = status
    response.json.return_value = body if body is not None else {}
    http.request.return_value = response
    return http


class FeatureFlagTests(unittest.TestCase):
    """The whole client is gated on CULINARY_OPS_ENABLED."""

    def test_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('CULINARY_OPS_ENABLED', None)
            self.assertFalse(_is_enabled())

    def test_enabled_truthy_values(self):
        for val in ('1', 'true', 'TRUE', 'yes', 'on', 'True'):
            with patch.dict(os.environ, {'CULINARY_OPS_ENABLED': val}):
                self.assertTrue(_is_enabled(), f'expected enabled for {val!r}')

    def test_enabled_falsy_values(self):
        for val in ('0', 'false', 'no', 'off', '', 'banana'):
            with patch.dict(os.environ, {'CULINARY_OPS_ENABLED': val}):
                self.assertFalse(_is_enabled(), f'expected disabled for {val!r}')


class DormantBehaviourTests(unittest.TestCase):
    """When the flag is off, every method must short-circuit cleanly."""

    def setUp(self):
        # Belt-and-suspenders: make sure no env state leaks into the test
        os.environ.pop('CULINARY_OPS_ENABLED', None)

    def test_get_returns_fallback_when_disabled(self):
        http = _mock_http(status=200, body={'should': 'never see this'})
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        result = client.get('/corp-manager/employees', fallback=lambda: ['fallback-data'])
        self.assertEqual(result, ['fallback-data'])
        # Critical: HTTP layer should NOT have been touched.
        http.request.assert_not_called()

    def test_get_returns_none_when_disabled_and_no_fallback(self):
        http = _mock_http()
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        self.assertIsNone(client.get('/corp-manager/employees'))
        http.request.assert_not_called()

    def test_post_disabled_short_circuits(self):
        http = _mock_http()
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        sentinel = object()
        result = client.post('/anything', {'a': 1}, fallback=lambda: sentinel)
        self.assertIs(result, sentinel)
        http.request.assert_not_called()


class EnabledHappyPathTests(unittest.TestCase):
    """When the flag is on and the backend returns 2xx, parsed JSON is returned."""

    def setUp(self):
        os.environ['CULINARY_OPS_ENABLED'] = '1'

    def tearDown(self):
        os.environ.pop('CULINARY_OPS_ENABLED', None)

    def test_get_returns_parsed_json(self):
        http = _mock_http(body={'employees': [{'id': 1}]})
        client = CulinaryOpsClient(
            base_url='https://api.example.com',
            http=http,
            session_getter=lambda: {},
        )
        result = client.get('/corp-manager/employees')
        self.assertEqual(result, {'employees': [{'id': 1}]})

        # Verify the request was constructed correctly.
        call = http.request.call_args
        self.assertEqual(call.args[0], 'GET')
        self.assertEqual(call.args[1], 'https://api.example.com/corp-manager/employees')
        self.assertIsNone(call.kwargs.get('params'))
        self.assertIsNone(call.kwargs.get('json'))
        self.assertEqual(call.kwargs.get('timeout'), 5.0)

    def test_get_with_query_params(self):
        http = _mock_http(body={'orders': []})
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        client.get('/corp-manager/orders', limit=50, status='paid')
        call = http.request.call_args
        self.assertEqual(call.kwargs['params'], {'limit': 50, 'status': 'paid'})

    def test_post_with_json_body(self):
        http = _mock_http(body={'ok': True, 'id': 'abc'})
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        result = client.post('/corp-manager/employees', {'email': 'a@b.com'})
        self.assertEqual(result, {'ok': True, 'id': 'abc'})
        call = http.request.call_args
        self.assertEqual(call.args[0], 'POST')
        self.assertEqual(call.kwargs['json'], {'email': 'a@b.com'})

    def test_patch_put_delete_dispatch(self):
        http = _mock_http(body={'ok': True})
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})

        client.patch('/corp-manager/employees/123', {'name': 'New'})
        self.assertEqual(http.request.call_args.args[0], 'PATCH')

        client.put('/corp-manager/par-levels', {'levels': []})
        self.assertEqual(http.request.call_args.args[0], 'PUT')

        client.delete('/corp-manager/employees/123', cascade=True)
        self.assertEqual(http.request.call_args.args[0], 'DELETE')
        self.assertEqual(http.request.call_args.kwargs['params'], {'cascade': True})

    def test_path_normalization_with_and_without_leading_slash(self):
        http = _mock_http(body={})
        client = CulinaryOpsClient(base_url='https://api.example.com/', http=http,
                                   session_getter=lambda: {})

        client.get('/corp-manager/employees')
        self.assertEqual(
            http.request.call_args.args[1],
            'https://api.example.com/corp-manager/employees',
        )

        client.get('corp-manager/employees')  # no leading slash
        self.assertEqual(
            http.request.call_args.args[1],
            'https://api.example.com/corp-manager/employees',
        )


class AuthHeaderTests(unittest.TestCase):
    """JWT pickup from session_getter."""

    def setUp(self):
        os.environ['CULINARY_OPS_ENABLED'] = '1'

    def tearDown(self):
        os.environ.pop('CULINARY_OPS_ENABLED', None)

    def test_jwt_added_when_present_in_session(self):
        http = _mock_http(body={})
        client = CulinaryOpsClient(
            http=http,
            session_getter=lambda: {'culinary_ops_jwt': 'abc.def.ghi'},
        )
        client.get('/corp-manager/employees')
        headers = http.request.call_args.kwargs['headers']
        self.assertEqual(headers['Authorization'], 'Bearer abc.def.ghi')
        self.assertEqual(headers['Accept'], 'application/json')

    def test_no_authorization_header_when_session_empty(self):
        http = _mock_http(body={})
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        client.get('/corp-manager/employees')
        headers = http.request.call_args.kwargs['headers']
        self.assertNotIn('Authorization', headers)

    def test_session_getter_exception_does_not_crash(self):
        http = _mock_http(body={})
        def boom():
            raise RuntimeError('no flask context')
        client = CulinaryOpsClient(http=http, session_getter=boom)
        # Should still complete the request, just without a JWT.
        client.get('/corp-manager/employees')
        headers = http.request.call_args.kwargs['headers']
        self.assertNotIn('Authorization', headers)


class FailureFallbackTests(unittest.TestCase):
    """All failure modes must call the fallback rather than raise."""

    def setUp(self):
        os.environ['CULINARY_OPS_ENABLED'] = '1'

    def tearDown(self):
        os.environ.pop('CULINARY_OPS_ENABLED', None)

    def test_transport_exception_falls_back(self):
        http = _mock_http(raise_exc=ConnectionError('boom'))
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        result = client.get(
            '/corp-manager/employees', fallback=lambda: 'fallback-value',
        )
        self.assertEqual(result, 'fallback-value')

    def test_500_status_falls_back(self):
        http = _mock_http(status=500, body={'error': 'kaboom'})
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        result = client.get(
            '/corp-manager/employees', fallback=lambda: ['gas-result'],
        )
        self.assertEqual(result, ['gas-result'])

    def test_404_status_falls_back(self):
        http = _mock_http(status=404, body={})
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        self.assertEqual(
            client.get('/no/such/path', fallback=lambda: 'gas'),
            'gas',
        )

    def test_403_status_falls_back(self):
        # cross-company access blocked → fall back to GAS rather than crash dashboard
        http = _mock_http(status=403, body={'error': 'forbidden'})
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        self.assertEqual(
            client.get('/corp-manager/employees', fallback=lambda: 'gas'),
            'gas',
        )

    def test_non_json_body_falls_back(self):
        http = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.side_effect = ValueError('not json')
        http.request.return_value = response
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        self.assertEqual(
            client.get('/corp-manager/employees', fallback=lambda: 'gas'),
            'gas',
        )

    def test_no_fallback_returns_none_on_failure(self):
        http = _mock_http(status=500)
        client = CulinaryOpsClient(http=http, session_getter=lambda: {})
        self.assertIsNone(client.get('/corp-manager/employees'))


class FactoryTests(unittest.TestCase):
    """get_culinary_client() returns a fresh client each call."""

    def test_factory_returns_client_instance(self):
        c = get_culinary_client()
        self.assertIsInstance(c, CulinaryOpsClient)

    def test_factory_returns_distinct_instances(self):
        # Important so per-request state can be added later without
        # accidentally sharing across requests.
        self.assertIsNot(get_culinary_client(), get_culinary_client())


if __name__ == '__main__':
    unittest.main()
