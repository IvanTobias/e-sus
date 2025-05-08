from typing import Callable
from flask import Request as FlaskRequest
from src.presentations.http_types import HttpRequest, HttpResponse


def request_adapter(flask_request: FlaskRequest, controller: Callable[[HttpRequest], HttpResponse]) -> HttpResponse:
    http_request = create_request_adapter(flask_request)
    http_response = controller(http_request)
    return http_response


def create_request_adapter(flask_request: FlaskRequest) -> HttpRequest:
    body = None
    if flask_request.data:
        body = flask_request.get_json(silent=True)

    return HttpRequest(
        body=body,
        headers=flask_request.headers,
        query_params=flask_request.args,
        path_params=flask_request.view_args,
        url=flask_request.full_path,
    )
