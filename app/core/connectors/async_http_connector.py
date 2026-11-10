# --- Standard Library Imports ---
from typing import Any, Optional

# --- Third-Party Imports ---
import aiohttp  # Async HTTP client session manager (non-blocking)


class AsyncHTTPConnector:
    """
    Asynchronous wrapper class for managing an HTTP connection/session using aiohttp.

    The connector does not log and does not have exceptions of its own: when something fails it raises a standard
    exception (or one of aiohttp) whose message says what failed, and whoever calls the connector decides what to do
    with the error and logs it, so that each error is logged only once.

    How the HTTP connection works
    -----------------------------
    HTTP runs over TCP. Before a request is sent, the program and the server have to establish a TCP connection, which
    is like opening a telephone line between the two. Opening it takes time: several round trips (the TCP handshake)
    and, with HTTPS, a few more (the TLS handshake, which sets up the encryption). Only then the request is sent.

    Over that line, a request is followed by a response:

        client  ---- request ---->  server
        client  <--- response ----  server

    The response has three parts: the status line (e.g., "200 OK"), the headers and the body. A connection handles
    only one request at a time. While the body of the response has not arrived and been consumed, the line is busy,
    and the next request on it would be mixed with the remaining bytes of the body.

    Opening a new connection for every request is expensive, so HTTP/1.1 has keep-alive: when a response ends, the
    line is not hung up, it stays open for the next request. The handshakes are only paid once.

    To take advantage of this, the HTTP libraries in general (e.g., 'requests', 'aiohttp') keep the open connections
    in a pool and reuse them, and this happens under the hood: the code that makes the requests does not usually see
    it. What changes from one library to another is the details (the size of the pool, when a connection goes back to
    it, what happens when it is full).

    In aiohttp, the session keeps a pool of connections (by default, up to 100): each one is either free or busy. A
    request takes a free connection of the same server from the pool, or opens a new one if there is none (and the
    pool is not full). A connection goes back to the pool only when its response has been consumed completely, by
    reading the body ('read()', 'json()', 'text()') or by calling 'release()' when the body is not needed. If a
    response is never read or released, its connection stays busy, and with many of them the pool can fill up and the
    new requests wait for a connection that nobody frees.

    Because of this, `request()` returns the response still open, and whoever calls it has to read or release it.
    """

    _LOG_PREFIX: str = "Async HTTP Connector |"

    _session: Optional[aiohttp.ClientSession]
    _base_url: str
    _verify_ssl: bool
    _server_configurations: dict

    def __init__(self, server_configurations: dict) -> None:
        """
        Initializes the HTTP transport parameters and the base URL.

        Args:
            server_configurations (dict): The configuration of the server, the 'connection' of the configuration of
                the system. It has either a 'url' or a 'host' (the other keys are optional). Examples:

                    # With a complete URL (it has priority over 'host', 'port' and 'schema'):
                    {
                        "url": "https://innov.cleanwattsdigital.com/api/2.0",
                        "verify_ssl": True,
                        "auth": {"username": "<username>", "password": "<password>"},
                        "headers": {"X-Api-Version": "2"}
                    }

                    # With the parts of the address (the 'schema' is "http" if it is not given, and without a 'port'
                    # the default port of the schema is used):
                    {"host": "energaize_inference_rh01", "port": 8002, "schema": "http"}

                'verify_ssl' is False if it is not given, and 'auth' and 'headers' are used by 'connect()' as the
                authentication and the headers of every request (the headers of one endpoint are given in 'request()').

        Raises:
            ValueError: If the configuration is not valid (see '_validate_server_configurations()').
            TypeError: If an option of the configuration has the wrong type (see '_validate_server_configurations()').
        """
        # The configuration is checked before anything is read from it, so the rest of the method can trust it.
        self._validate_server_configurations(server_configurations)

        # Keeps the whole configuration of the server, because 'connect()' still needs the 'auth' and 'headers' keys.
        self._server_configurations = server_configurations

        # The parts of the address of the server. 'host' and 'port' are optional here: they are only used if there is
        # no 'url'. The 'schema' (the "http" or "https" of the address) is "http" when it is not configured.
        _host = server_configurations.get("host")
        _port = server_configurations.get("port")
        _schema: str = server_configurations.get("schema", "http")

        # The base URL is the start of the address shared by all the requests; the endpoint of each request is added
        # to it later (see 'request()').
        if server_configurations.get("url"):
            # Case 1: a complete 'url' was given, so it is used as it is (it has priority over 'host' and 'port').
            # The "/" at the end is removed so that it does not duplicate when the endpoint is added.
            self._base_url = server_configurations.get("url").rstrip("/")
        elif _port:
            # Case 2: no 'url', so the address is built from the 'host' (the validation guarantees there is one).
            # With a 'port', it is added after the host (e.g., "http://energaize_inference_rh01:8002").
            self._base_url = f"{_schema}://{_host}:{_port}"
        else:
            # Case 3: no 'url' and no 'port', so the default port of the schema is used (e.g., "https://example.com").
            self._base_url = f"{_schema}://{_host}"

        # Whether the SSL/TLS certificate of the server is verified. It is False by default, i.e., the certificate is
        # not verified unless 'verify_ssl' is set to True in the configuration.
        self._verify_ssl = server_configurations.get("verify_ssl", False)

        # The session is not created here: aiohttp wants it created inside the running event loop, so it is created in
        # 'connect()' (which is async). Until then, there is no session.
        self._session = None

    def _validate_server_configurations(self, server_configurations: dict) -> None:
        """
        Checks the configuration of the server before the connector is created.

        Raises:
            ValueError: If the configuration has neither 'url' nor 'host', if 'port' is not between 1 and 65535, if
                the username of 'auth' has a ":", or if 'auth' has only one of the username and the password.
            TypeError: If 'url', 'host' or 'schema' is not a string, if 'port' is not an integer, if 'verify_ssl' is
                not a boolean, if 'auth' or 'headers' is not a dictionary, or if the username or the password of
                'auth' is not a string.
        """
        _port = server_configurations.get("port")
        _verify_ssl = server_configurations.get("verify_ssl", False)
        _auth = server_configurations.get("auth")
        _auth = _auth if isinstance(_auth, dict) else {}

        # Guard Clause 1: Checks that the connector knows where to connect: there has to be a 'url' or a 'host'.
        if not server_configurations.get("url") and not server_configurations.get("host"):
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing HTTP setup: Either 'url' or 'host' must be provided."
            )

        # Guard Clause 2: Checks the type of every option. All of them are optional: a missing key or a null value is
        # accepted, except for 'verify_ssl', which can be missing (it is False then) but not null. (That there is a
        # 'url' or a 'host' was already checked in Guard Clause 1.) A boolean is not accepted as the port, even though
        # Python sees True as the integer 1.
        # The message only says the type of the wrong value, and never the value, because it can be a credential and
        # the message ends up in the logs.
        for name, value, expected_type, description, optional in (
                ("url", server_configurations.get("url"), str, "a string", True),
                ("host", server_configurations.get("host"), str, "a string", True),
                ("schema", server_configurations.get("schema"), str, "a string", True),
                ("port", _port, int, "an integer", True),
                ("verify_ssl", _verify_ssl, bool, "a boolean", False),
                ("auth", server_configurations.get("auth"), dict, "a dictionary", True),
                ("headers", server_configurations.get("headers"), dict, "a dictionary", True),
                ("auth.username", _auth.get("username"), str, "a string", True),
                ("auth.password", _auth.get("password"), str, "a string", True),
        ):
            if value is None and optional:
                continue
            if isinstance(value, expected_type) and not (isinstance(value, bool) and expected_type is not bool):
                continue
            raise TypeError(
                f"{self._LOG_PREFIX} Error initializing HTTP setup: '{name}' must be {description}, and it is "
                f"{type(value).__name__}."
            )

        # Guard Clause 3: Checks that the 'port', when given, is a valid TCP port (between 1 and 65535).
        if _port is not None and not 1 <= _port <= 65535:
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing HTTP setup: 'port' must be between 1 and 65535, "
                f"and it is {_port!r}."
            )

        # Guard Clause 4: Checks that the username has no ":". The Basic authentication joins the username and the
        # password as "username:password", so a ":" in the username would be read as the separator.
        if ":" in (_auth.get("username") or ""):
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing HTTP setup: 'auth.username' cannot have a \":\"."
            )

        # Guard Clause 5: Checks that 'auth' has both the username and the password, or neither. 'connect()' only uses
        # the authentication when it has both, so with only one of them it would be turned off without any warning.
        if bool(_auth.get("username")) != bool(_auth.get("password")):
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing HTTP setup: 'auth' must have both 'username' and 'password', "
                f"or neither."
            )

    async def connect(self) -> None:
        """
        Establishes the persistent, non-blocking HTTP ClientSession.

        The configuration was already checked in '__init__', so nothing is expected to fail here. If something does
        (an unexpected error of aiohttp), it is not caught: the error goes up as it is, because wrapping it would only
        add a phrase to the same error.

        Raises:
            RuntimeError: If the connector is already connected.
        """
        # Guard Clause 1: Checks that there is no open session yet. A second 'connect()' would create another session
        # and the first one would be lost without being closed (with its connections still open).
        if self.is_connected():
            raise RuntimeError(f"{self._LOG_PREFIX} The connector is already connected.")

        # 1. Create the TCPConnector (the transport).
        # The TCPConnector opens the TCP/TLS connections to the server and keeps the pool of connections (see the
        # class documentation): it holds the open connections, reuses a free one or opens a new one for each
        # request, and limits the total to 100 (its 'limit' parameter, which is not set here, so the default is
        # used). It does not make HTTP requests by itself, it is only the transport.
        #
        # The connector is created by hand only to pass it the SSL/TLS verification ('ssl'), which is a property
        # of the connections. If this were not needed, it could be left out: the ClientSession creates its own
        # TCPConnector, with the default values, when it does not receive one.
        #
        # In general, a connector (in aiohttp) or an adapter mounted on the session (in requests) is only needed
        # to change something of the connections: the pool (its size, what happens when it is full) or other
        # properties, such as the SSL/TLS verification (here) or the automatic retries (in requests). If the
        # default values are enough, the session alone is enough.
        #
        # For comparison, the same idea with the 'requests' library (which is blocking) would be:
        #
        #   session = requests.Session()                  # like the ClientSession
        #   adapter = HTTPAdapter(pool_maxsize=100)       # like the TCPConnector (the pool)
        #   session.mount("http://", adapter)
        #   session.mount("https://", adapter)
        #
        # The differences: 'requests' reads the whole body in the call, so the connection goes back to the pool
        # by itself (unless 'stream=True'), and when the pool is full it opens an extra connection and discards
        # it afterwards, instead of making the request wait (unless 'pool_block=True').
        connector = aiohttp.TCPConnector(ssl=self._verify_ssl)

        # 2. Extract baseline credentials
        _auth_config: dict = self._server_configurations.get("auth") or {}
        _username = _auth_config.get("username")
        _password = _auth_config.get("password")
        auth = aiohttp.BasicAuth(_username, _password) if _username and _password else None

        # 3. Get the headers that go in every request, from the 'headers' of the configuration of the server. There
        # are no default headers on purpose: the connector is generic and does not assume what the server accepts.
        _headers = self._server_configurations.get("headers") or {}

        # 4. Create the ClientSession (what makes the requests).
        # The ClientSession is the object used to make the requests: it holds what is the same in all of them
        # (the headers and the authentication above) and sends each request through the connector it receives,
        # which is where the connections come from.
        #
        #   ClientSession  -> what is asked (URL, method, headers, authentication)
        #     └─ TCPConnector -> where it goes through (the open connections, the pool)
        #
        # There is a single session for the whole life of this connector, and not one per request, because the pool
        # lives inside the connector of the session: a new session for each request would lose the reuse of the
        # connections. Closing the session (see 'close()') also closes the connector and all its connections.
        self._session = aiohttp.ClientSession(
            connector=connector,
            auth=auth,
            headers=_headers
        )

    async def request(self, method: str, endpoint: str, **kwargs: Any) -> aiohttp.ClientResponse:
        """
        Executes a non-blocking, asynchronous HTTP request.

        The response is returned open, so its connection stays busy. It only goes back to the pool when the caller reads
        the body or releases the response (see the class documentation).

        Args:
            method (str): The HTTP method, in upper or lower case. Examples: "GET", "POST", "post".
            endpoint (str): The path of the request, added to the base URL of the server. It starts with "/" and can
                have a query string. Examples: "/inference", "/assets/12/observations?window_seconds=900".
            **kwargs: Options to add to the ones of the session, for the current request only. They are the ones of
                aiohttp. Examples: {"headers": {"Content-Type": "application/json"}, "timeout": 5.0,
                "json": {"features": [1.5, 2.0]}}. 'headers' can be None (the headers of the session are used), and
                'timeout' is a number of seconds (the input handler converts the 'timeout_ms' of the endpoint, e.g.,
                5000 ms becomes 5.0).

        Returns:
            aiohttp.ClientResponse: The response, when the status is 2xx.

        Raises:
            RuntimeError: If the connector is not connected.
            TypeError: If the 'timeout' is not a number or an aiohttp.ClientTimeout.
            ValueError: If the 'timeout' is a number that is not greater than 0.
            asyncio.TimeoutError: If the request takes longer than its timeout (its message is empty). From Python
                3.11 it is the same as the built-in TimeoutError.
            aiohttp.ClientResponseError: If the status is 3xx, 4xx or 5xx ('status' has the code).
            aiohttp.ClientError: For the other failures of aiohttp (e.g., the server cannot be reached).
        """
        # Guard Clause 1: Checks that there is an open session.
        if not self.is_connected():
            raise RuntimeError(f"{self._LOG_PREFIX} ClientSession is closed or unallocated.")

        # The full address of the request: the base URL of the server plus the path of the endpoint. The "/" at the
        # start of the endpoint is removed and one is put between the two, so that there is always exactly one,
        # whether the endpoint is written as "/v1/data" or "v1/data" (e.g., "http://host:8002" + "/inference").
        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        # The HTTP methods are written in upper case ("GET"), so this accepts "get" or "Get" as well.
        method_upper = method.upper()

        # The 'timeout' of the request. What aiohttp does with each kind of value, and what is done here:
        #   - Not passed: aiohttp uses the timeout of the session, which here is its default (5 minutes in total).
        #   - A 'ClientTimeout': used as it is. It lets the limits be separate (e.g., for connecting and for reading).
        #   - A number (e.g., 5 or 2.5): the maximum number of seconds that the whole request can take. aiohttp itself
        #     would turn any value that is not a 'ClientTimeout' into 'ClientTimeout(total=value)', where 'total' is the
        #     limit for everything (connecting, sending and receiving), without checking what the value is. The
        #     conversion is done here instead, so that the meaning of a number is written in this code and does not
        #     depend on that implicit behaviour of aiohttp.
        #   - Anything else: refused by the Guard Clause below, because aiohttp would not refuse it.
        if "timeout" in kwargs:
            _timeout = kwargs["timeout"]
            # Guard Clause 2: Checks that the 'timeout' is a number of seconds or a 'ClientTimeout'. What aiohttp would
            # do with the values that are refused:
            #   - A text, a list, etc.: fail later, inside aiohttp, with an unclear message (e.g., "'>' not supported
            #     between instances of 'str' and 'int'").
            #   - None, 0 or a negative number: silently remove the limit of the whole request, which could then wait
            #     without a time limit.
            #   - A 'bool': Python takes True as the number 1, so it would be a timeout of 1 second.
            if not isinstance(_timeout, aiohttp.ClientTimeout):
                if isinstance(_timeout, bool) or not isinstance(_timeout, (int, float)):
                    raise TypeError(
                        f"{self._LOG_PREFIX} 'timeout' must be a number of seconds or an aiohttp.ClientTimeout, and it "
                        f"is {type(_timeout).__name__}."
                    )
                if _timeout <= 0:
                    raise ValueError(
                        f"{self._LOG_PREFIX} 'timeout' must be greater than 0 seconds, and it is {_timeout}."
                    )
                kwargs["timeout"] = aiohttp.ClientTimeout(total=_timeout)

        # 'kwargs' go to 'aiohttp.ClientSession.request' as they are. Each option of aiohttp can be in one of three
        # places, and this decides what can be passed here:
        #   - Only in the session: set once, when the session is created in 'connect()', and not accepted here. For
        #     example, the 'connector' (the pool and the SSL/TLS verification), 'cookie_jar' and 'trace_configs'.
        #   - Only in the request (kwargs): what is different in each request. For example, 'json', 'data', 'params',
        #     'allow_redirects' and 'compress'.
        #   - In both: the session has a default and the request can set its own, which is the one used in that
        #     request. For example, 'headers', 'auth', 'cookies', 'timeout', 'proxy' and 'raise_for_status'. Here the
        #     session has the 'auth' and the 'headers' of the configuration of the server, and the request usually
        #     passes the 'timeout' and the 'headers' that depend on the route.
        #
        # The case of the 'headers' (in both) is that the ones of the request are added to the ones of the session, and
        # do not replace them:
        #   - A name that only exists in the session is sent in every request.
        #   - A name that only exists in the request is added to that request only, and is not kept for the next ones.
        #   - A name that exists in both is sent once, with the value of the request, which wins.
        #
        # Who compares the names: aiohttp, not this code. The names of HTTP headers do not distinguish upper and lower
        # case ("x-both" and "X-Both" are the same header), and aiohttp keeps the headers in a structure that ignores
        # the case (a 'CIMultiDict'), which can be seen as a notebook where a name can only appear once. There are two
        # different situations:
        #
        #   1. Headers of the session + headers of the request. Example:
        #        session: {"X-Both": "session"}
        #        request: {"x-both": "request"}
        #      aiohttp copies the headers of the session to the notebook ("X-Both: session"), and then writes the ones
        #      of the request on top. The notebook sees that "x-both" is a name it already has, and replaces the value.
        #      Sent: "X-Both: request" (a single header).
        #
        #   2. Two names that only differ in case in the headers of the configuration of the server. Example:
        #        session: {"Accept": "application/json", "accept": "text/xml"}
        #      Here there is no "write on top" step: when the session is created, aiohttp only copies all the entries
        #      to the notebook, one after the other. Both stay.
        #      Sent: "Accept: application/json" and "accept: text/xml" (two headers).
        #
        # The difference is how aiohttp is written, and not a rule of HTTP. Because of it, each header must be written
        # only once in the configuration.
        #
        # The errors of aiohttp (e.g., the server cannot be reached) and the timeout are not caught: they go up as they
        # are, and whoever calls decides what to do with each one (e.g., repeat the request after a timeout).
        response = await self._session.request(method=method_upper, url=url, **kwargs)

        # Guard Clause 3: Checks that the status is a success. The start of the body is added to the message, because
        # it usually says why the server refused. 'errors="replace"' keeps a body that is not valid text from hiding
        # the status.
        if 300 <= response.status < 600:
            response_text = await response.text(errors="replace")
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message=f"{self._LOG_PREFIX} HTTP error during async {method_upper} for {url}: {response_text[:500]}",
                headers=response.headers
            )

        return response

    async def close(self) -> None:
        """Closes the HTTP ClientSession cleanly."""
        if self._session:
            try:
                await self._session.close()
            finally:
                # Even if closing fails, the session is not used again
                self._session = None

    def is_connected(self) -> bool:
        """Returns True if the session exists and is not closed."""
        return self._session is not None and not self._session.closed