import sys
import requests
import urllib3
from logging import getLogger

logger = getLogger(__name__)

# FIX: suppress SSL warnings since verify=False is used
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _auth_headers(token: str) -> dict[str, str]:
    """Return the Authorization header dict for a given token."""
    return {"Authorization": f"Basic {token}"}


def _raise_for_status(response: requests.Response, context: str) -> None:
    """Raise a RuntimeError with context if the response is not 2xx."""
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"{context} failed — HTTP {response.status_code}: {response.text}"
        ) from exc


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------
def _authenticate(
    base_url: str,
    username: str,
    password: str,
) -> tuple[str, str, str]:
    """
    Log in and return (auth_token, tenant_id, tenant_name).
    The token is NOT logged to avoid leaking credentials.
    """
    url = f"{base_url}/api/framework/auth/login"
    payload = {
        "userEmail": username,
        "submittedPassword": password,
        "expirySeconds": 36000,
    }
    response = requests.post(url, json=payload, verify=False)
    _raise_for_status(response, "Authentication")

    body = response.json()
    token = body["AuthToken"]
    tenant_id = str(body["TenantId"])
    tenant_name = str(body["TenantName"])

    # Token intentionally omitted from log
    logger.info("Authenticated — TenantId=%s  TenantName=%s", tenant_id, tenant_name)
    return token, tenant_id, tenant_name


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------
def run_pipeline(
    base_url: str,
    username: str,
    password: str,
    pipeline: str,
) -> None:

    base_url = base_url.rstrip("/")
    if base_url.endswith("/api"):
        base_url = base_url[:-4]
    if not base_url.startswith("https://"):
        base_url = "https://" + base_url

    # 1. Authenticate.
    token, tenant_id, tenant_name = _authenticate(base_url, username, password)
    auth_headers = _auth_headers(token)

    # 2. Get workspace.
    url = f"{base_url}/api/v3/integration/{tenant_name}/Workspaces"
    response = requests.get(url, headers=auth_headers, verify=False)
    _raise_for_status(response, "Get Workspaces")
    data = response.json()
    if not data:
        logger.warning("No workspace found for this tenant")
        raise Exception("No workspace found for this tenant")
    workspace_id = data[0]["Id"]

    # 3. Get pipelines.
    url = f"{base_url}/api/v3/integration/{tenant_name}/Workspaces/{workspace_id}/pipelines"
    response = requests.get(url, headers=auth_headers, verify=False)
    _raise_for_status(response, "Get Pipelines")
    data = response.json()
    if not data:
        logger.warning("No pipelines found for this tenant")
        raise Exception("No pipelines found for this tenant")

    # FIX: initialise pipeline_id to None before loop to avoid UnboundLocalError
    pipeline_id = None
    for _ppl in data:
        # FIX: .trim() does not exist in Python — use .strip()
        if _ppl["Name"].strip().lower() == pipeline.strip().lower():
            pipeline_id = _ppl["Id"]
            break  # stop once matched

    if not pipeline_id:
        raise Exception(f"No pipeline named '{pipeline}' found for this tenant")

    # 4. Execute pipeline.
    _url = f"{base_url}/api/v3/integration/{tenant_name}/ui/airflow/execute?IsolatedExecution=false"
    json_payload = {
        "TenantId": tenant_id,
        "WorkspaceId": workspace_id,
        "PipelineId": pipeline_id,
    }
    response = requests.post(_url, headers=auth_headers, json=json_payload, verify=False)
    _raise_for_status(response, "Execute Pipeline")
    logger.info("Pipeline '%s' triggered successfully.", pipeline)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(argv: list[str]) -> tuple[str, str, str, str]:
    if len(argv) != 5:
        # FIX: corrected usage string to match actual 4 arguments
        print(
            "Usage: python run_pipeline.py "
            "<LS_URL> <LS_Username> <LS_Password> <Pipeline>"
        )
        sys.exit(1)
    _, base_url, username, password, pipeline = argv
    return base_url, username, password, pipeline


if __name__ == "__main__":
    _base_url, _username, _password, _pipeline = _parse_args(sys.argv)
    try:
        run_pipeline(_base_url, _username, _password, _pipeline)
    except (RuntimeError, KeyError, requests.RequestException) as err:
        logger.error("Pipeline execution failed: %s", err)
        sys.exit(1)

    sys.exit(0)