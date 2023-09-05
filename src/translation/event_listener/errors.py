from http import HTTPStatus


def error_response(msg, status):
    return {"error": msg}, status


NO_BODY = error_response("pushed message without body", HTTPStatus.BAD_REQUEST)
INVALID_MESSAGE_FORMAT = error_response(
    "invalid message format", HTTPStatus.BAD_REQUEST
)
SUBSCRIPTION_AND_MESSAGE_REQUIRED = error_response(
    "pushed message must contain subscription and message field", HTTPStatus.BAD_REQUEST
)
NO_ROUTING_RULE_FOUND = error_response(
    "no message routing rule found, defaulting to no action", HTTPStatus.OK
)
INVALID_FORMAT_OR_SUBFOLDER_FOUND = error_response(
    "Please ensure you upload only JSON configuration files to trigger DMT workflow in the folders config/[ddl, dml, sql,data] paths only",
    HTTPStatus.OK,
)
