from fastapi.requests import Request
from fastapi.responses import Response
from starlette.middleware.base import (BaseHTTPMiddleware,
                                       RequestResponseEndpoint)
from starlette.types import ASGIApp
from random import randrange


class BlockHosts(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Do not authenticate or throttle the following endpoints:
        if request.url.path in ("/status", "/openapi.json", "/docs"):
            return await call_next(request)

        # Authenticate not so safe api_key is valid.
        not_so_safe_api_key = "ADU8S67Ddy!d7f?"
        if "api_key" not in request.query_params._dict \
            or request.query_params._dict["api_key"] != not_so_safe_api_key:
            return Response(
                content="Forbidden",
                status_code=403
            )

        # Mimic an unreliable API connection.
        mimic_throttle_rate = randrange(0, 100)
        throttle_threshold = 80
        if mimic_throttle_rate > throttle_threshold:
            return Response(
                content="Too many requests",
                status_code=429
            )

        return await call_next(request)
