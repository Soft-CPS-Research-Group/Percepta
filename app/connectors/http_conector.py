import requests
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout


class HTTPErrorWrapper(Exception):
    """Custom exception for HTTP-related errors."""
    pass


class HTTPConnector:
    """
    Wrapper class for managing an HTTP connection/session.
    Provides methods for GET, POST, and session management with structured exception handling.
    """

    _session: requests.Session  # Session object
    _base_url: str  # Base URL of the HTTP server

    def __init__(self, url : str):
        """
        Initialize HTTP connection parameters.

        Args:
            url (str): Server base url.
        """
        try:
            self._base_url = url
            if not self._base_url:
                raise ValueError("Base URL must be provided")

            self._session = requests.Session()

        except Exception as e:
            raise HTTPErrorWrapper(f"Error initializing HTTP session: {e}") from e

    # TODO: review timeout settings)
    def _request(self, method: str, endpoint: str, **kwargs):
        """
        Internal method to perform HTTP requests with error handling and JSON validation.

        Args:
            method (str): HTTP method, e.g., 'GET', 'POST'.
            endpoint (str): API endpoint to call (appended to base_url).
            **kwargs: Additional arguments to pass to requests (params, data, json, timeout, etc.)

        Returns:
            Response: The raw HTTP response object from requests.

        Raises:
            HTTPErrorWrapper: For connection errors, timeouts, HTTP status 3xx–5xx, or invalid JSON.
        """

        url = f"{self._base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        try:
            response = self._session.request(method=method, url=url, **kwargs)

            if 300 <= response.status_code < 600:
                raise HTTPErrorWrapper(
                    f"HTTP error during {method.upper()} request for {url}: "
                    f"status code {response.status_code}, response: {response.text[:500]}"
                )


            return response

        except ConnectionError as e:
            raise HTTPErrorWrapper(f"Connection error during {method.upper()} request for {url}: {e}") from e
        except Timeout as e:
            raise HTTPErrorWrapper(f"Timeout during {method.upper()} request for {url}: {e}") from e
        except Exception as e:
            raise HTTPErrorWrapper(f"Unexpected error during {method.upper()} request for {url}: {e}") from e

    def get(self, endpoint, timeout, params=None):
        """
        Perform a GET request.

        Args:
            endpoint (str): API endpoint to call (appended to base_url).
            params (dict, optional): Query parameters.
            timeout (int, optional): Timeout in seconds. Defaults to 10.

        Returns:
            Response: The raw HTTP response object from requests.

        Raises:
            HTTPErrorWrapper: For connection errors, timeouts, HTTP status 3xx–5xx, or invalid JSON.
        """
        return self._request("GET", endpoint, params=params, timeout=timeout)

    def post(self, endpoint : str, data : dict = None, timeout : int =10):
        """
        Perform a POST request.

        Args:
            endpoint (str): API endpoint to call (appended to base_url).
            data (dict, optional): JSON payload.
            timeout (int, optional): Timeout in seconds. Defaults to 10.

        Returns:
            Response: The raw HTTP response object from requests..

        Raises:
            HTTPErrorWrapper: For connection errors, timeouts, HTTP status 3xx–5xx, or invalid JSON.
        """
        return self._request("POST", endpoint, json=data, timeout=timeout)

    def put(self, endpoint : str, data : dict = None, timeout : int =10):
        """
        Perform a POST request.

        Args:
            endpoint (str): API endpoint to call (appended to base_url).
            data (dict, optional): JSON payload.
            timeout (int, optional): Timeout in seconds. Defaults to 10.

        Returns:
            Response: The raw HTTP response object from requests..

        Raises:
            HTTPErrorWrapper: For connection errors, timeouts, HTTP status 3xx–5xx, or invalid JSON.
        """
        return self._request("PUT", endpoint, json=data, timeout=timeout)


    def update_headers(self, new_headers: dict):
        """
        Update session headers dynamically.

        Args:
            new_headers (dict): Dictionary of headers to add or update.
        """
        try:
            if not isinstance(new_headers, dict):
                raise ValueError("Headers must be provided as a dictionary")

            if new_headers != dict(self._session.headers):
                self._session.headers.update(new_headers)
        except Exception as e:
            raise HTTPErrorWrapper(f"Failed to update session headers: {e}") from e

    def close(self):
        """Close the HTTP session cleanly."""
        try:
            self._session.close()
        except Exception as e:
            raise HTTPErrorWrapper(f"Failed to close HTTP session: {e}") from e


    def is_connected(self) -> bool:
        """
        Returns True if  the connection is still alive.
        """
        if not hasattr(self, "_session") or self._session is None:
            return False

        return True