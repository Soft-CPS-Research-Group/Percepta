import requests
from urllib.parse import urlparse
from requests.exceptions import ConnectionError, Timeout


class HTTPErrorWrapper(Exception):
    """Custom exception for HTTP-related errors."""
    pass


class HTTPConnector:
    """
    Wrapper class for managing an HTTP connection/session.
    Provides methods for GET, POST, PUT and session management with structured exception handling.
    """

    _session: requests.Session  # Session object
    _base_url: str  # Base URL of the HTTP server

    def __init__(self, url: str):
        """
        Initializes HTTP connection parameters.

        Args:
            url (str): Server base URL.
        """
        try:
            self._base_url = url.strip()
            if not self._base_url:
                raise ValueError("Base URL must be provided")

            # Validate the URL structure
            if not self._is_valid_url(self._base_url):
                raise ValueError(f"Invalid URL format: {self._base_url}")

            # Create a persistent HTTP session
            self._session = requests.Session()

            self._session.verify = False # TODO para fase de testes

        except Exception as e:
            raise HTTPErrorWrapper(f"Error initializing HTTP session: {e}") from e

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """
        Checks if the provided URL is syntactically valid.

        Args:
            url (str): URL to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        parsed = urlparse(url)
        return all([parsed.scheme in ("http", "https"), parsed.netloc])

    # TODO: review timeout settings
    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Performs HTTP requests with error handling and JSON validation.

        Args:
            method (str): HTTP method, e.g., 'GET', 'POST'.
            endpoint (str): API endpoint to call (appended to base_url).
            **kwargs: Additional arguments to pass to requests (params, data, json, timeout, etc.)
        """

        # Build the full URL by combining base_url and endpoint
        url = f"{self._base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        try:
            # Send the HTTP request using the session object
            response = self._session.request(method=method, url=url, **kwargs)
            # Check if the response status code indicates an error (3xx, 4xx, 5xx)
            if 300 <= response.status_code < 600:
                # Raise a custom wrapped error with details about the failed request
                raise HTTPErrorWrapper(
                    f"HTTP error during {method.upper()} request for {url}: "
                    f"status code {response.status_code}, response: {response.text[:500]}"
                )

            # If no error, return the response object
            return response

        # Handle connection-related errors
        except ConnectionError as e:
            raise HTTPErrorWrapper(f"Connection error during {method.upper()} request for {url}: {e}") from e

        # Handle request timeout errors
        except Timeout as e:
            raise HTTPErrorWrapper(f"Timeout during {method.upper()} request for {url}: {e}") from e

        # Handle any other unexpected errors
        except Exception as e:
            raise HTTPErrorWrapper(f"Unexpected error during {method.upper()} request for {url}: {e}") from e

    def get(self, endpoint, timeout, params=None) -> requests.Response:
        """
        Performs a GET request using _request.

        Args:
            endpoint (str): API endpoint to call (appended to base_url).
            params (dict, optional): Query parameters.
            timeout (int, optional): Timeout in seconds. Defaults to 10.
        """
        return self._request("GET", endpoint, params=params, timeout=timeout)

    def post(self, endpoint : str, data : dict = None, timeout : int =10) -> requests.Response:
        """
        Performs a POST request using _request.

        Args:
            endpoint (str): API endpoint to call (appended to base_url).
            data (dict, optional): JSON payload.
            timeout (int, optional): Timeout in seconds. Defaults to 10.
        """
        return self._request("POST", endpoint, json=data, timeout=timeout)

    def put(self, endpoint : str, data : dict = None, timeout : int =10) -> requests.Response:
        """
        Performs a POST request using _request.

        Args:
            endpoint (str): API endpoint to call (appended to base_url).
            data (dict, optional): JSON payload.
            timeout (int, optional): Timeout in seconds. Defaults to 10.
        """
        return self._request("PUT", endpoint, json=data, timeout=timeout)

    def update_headers(self, new_headers: dict) -> None:
        """
        Updates session headers dynamically.

        Args:
            new_headers (dict): Dictionary of headers to add or update.
        """
        try:
            # Ensure the provided argument is a dictionary
            if not isinstance(new_headers, dict):
                raise ValueError("Headers must be provided as a dictionary")

            # Only update headers if they are different from the current session headers
            if new_headers != dict(self._session.headers):
                self._session.headers.update(new_headers)  # Merge/overwrite headers in the session

        # Handle any exception that occurs during the header update
        except Exception as e:
            raise HTTPErrorWrapper(f"Failed to update session headers: {e}") from e

    def close(self) -> None:
        """Closes the HTTP session cleanly."""
        try:
            # Attempt to close the underlying requests.Session object
            self._session.close()

        # Handle any exception that occurs during the session close
        except Exception as e:
            raise HTTPErrorWrapper(f"Failed to close HTTP session: {e}") from e

    def is_connected(self) -> bool:
        """
        Returns True if the connection is still alive.
        """
        # Check if the instance has a '_session' attribute and that it is not None
        if not hasattr(self, "_session") or self._session is None:
            return False  # No active session

        return True  # Session exists and is assumed to be active