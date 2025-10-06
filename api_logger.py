# api_logger.py
import time
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from database import get_db, log_request

class APILoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        request_body = await request.body()
        request_size = len(request_body)

        response = await call_next(request)

        # Compute size of the response body
        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk
        response_size = len(response_body)

        # Log the request and response details
        process_time = round(time.time() - start_time, 4)
        db = next(get_db())
        log_request(
            session=db,
            method=request.method,
            url=str(request.url),
            client_ip=request.client.host,
            request_size=request_size,
            response_size=response_size,
            response_status=response.status_code
        )

        # Rebuild the response (since body_iterator was consumed)
        return Response(content=response_body, status_code=response.status_code, headers=dict(response.headers))

