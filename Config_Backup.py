import sys, re
from datetime import datetime
import requests


JSON_HEADERS = {"Content-Type": "application/json"}


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


def sanitize_isoformat(created_at: str) -> str:
    s = created_at.replace("Z", "+00:00")
    # Pad OR truncate fractional seconds to exactly 6 digits
    s = re.sub(
        r'\.(\d+)([+-]\d{2}:\d{2}|$)',
        lambda m: f".{m.group(1)[:6].ljust(6, '0')}{m.group(2)}",
        s
    )
    return s
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
    response = requests.post(url, json=payload, headers=JSON_HEADERS, verify=False)
    _raise_for_status(response, "Authentication")

    body = response.json()
    token = body["AuthToken"]
    tenant_id = str(body["TenantId"])
    tenant_name = str(body["TenantName"])

    print("Authenticated — TenantId=%s  TenantName=%s", tenant_id, tenant_name)
    return token, tenant_id, tenant_name


def _get_workflow_id_from_last_actions(
    base_url: str,
    tenant_id: str,
    token: str,
) -> str | None:
    """Try to find the backup workflow ID from the last-actions endpoint."""
    url = f"{base_url}/api/workflow/tenant/{tenant_id}/lastactions"
    response = requests.get(url, headers=_auth_headers(token), verify=False)
    _raise_for_status(response, "Last-actions fetch")

    actions = response.json()
    if not actions:
        return None

    for step in actions[0].get("LastAction", {}).get("Steps", []):
        remarks = step.get("Remarks", "")
        if remarks.endswith("tenant_config.zip"):
            parts = remarks.rsplit("/", 2)
            if len(parts) >= 2:
                wid_expr = parts[1]
                if wid_expr.startswith("o9"):
                    return wid_expr.rsplit(".", 1)[-1]

    return None


def _get_workflow_id_from_backups(
    base_url: str,
    tenant_name: str,
    token: str,
) -> str | None:
    """Find the most-recent backup workflow ID from the backups list endpoint."""
    url = f"{base_url}/api/tenants/{tenant_name}/backups"
    response = requests.get(url, headers=_auth_headers(token), verify=False)
    _raise_for_status(response, "Backups list fetch")

    latest_dt: datetime | None = None
    workflow_id: str | None = None

    for backup in response.json():
        created_at = backup.get("CreatedAt")
        if not created_at:
            continue
        dt = datetime.fromisoformat(sanitize_isoformat(created_at))
        if latest_dt is None or dt > latest_dt:
            latest_dt = dt
            workflow_id = backup.get("WorkflowId")

    return workflow_id


def _get_download_url(
    base_url: str,
    tenant_name: str,
    workflow_id: str,
    token: str,
) -> str | None:
    """Return the pre-signed download URL for the backup ZIP, or None."""
    url = (
        f"{base_url}/api/tenants/{tenant_name}"
        f"/backups/{workflow_id}/activity/BackupTenantConfigActivity"
    )
    response = requests.get(url, headers=_auth_headers(token), verify=False)
    _raise_for_status(response, "Download-URL fetch")
    return response.json() or None


def _download_zip(
    download_url: str,
    output_path: str,
) -> None:
    """Stream-download the ZIP to *output_path*."""
    response = requests.get(
        download_url, allow_redirects=True, stream=True, verify=False, timeout=300
    )
    _raise_for_status(response, "ZIP download")

    with open(output_path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=8192):
            fh.write(chunk)

    print("Backup saved to: %s", output_path)


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------
def backup_config(
    base_url: str,
    username: str,
    password: str,
    customer: str,
    sftp_folder: str,
) -> None:
    """
    Authenticate against *base_url*, locate the latest tenant config backup,
    and download it as ``ConfigBackup_<customer>_<sftp_folder>.zip``.

    Args:
        base_url:    Root URL of the LeanSwift instance (no trailing slash).
        username:    Login e-mail address.
        password:    Login password.
        customer:    Customer identifier used in the output filename.
        sftp_folder: Folder name used in the output filename.
    """

    # 1. Authenticate.
    token, tenant_id, tenant_name = _authenticate(base_url, username, password)

    # 2. Resolve workflow ID — try fast path first, then fall back.
    workflow_id = _get_workflow_id_from_last_actions(base_url, tenant_id, token)
    if not workflow_id:
        print("Last-actions did not yield a workflow ID; trying backups list …")
        workflow_id = _get_workflow_id_from_backups(base_url, tenant_name, token)

    if not workflow_id:
        raise RuntimeError("Could not determine the backup workflow ID.")

    print("Resolved workflow_id: %s", workflow_id)

    # 3. Fetch the pre-signed download URL.
    download_url = _get_download_url(base_url, tenant_name, workflow_id, token)
    if not download_url:
        raise RuntimeError("Backup activity returned an empty download URL.")

    # 4. Download the ZIP.
    output_path = f"ConfigBackup_{customer}_{sftp_folder}.zip"
    _download_zip(download_url, output_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(argv: list[str]) -> tuple[str, str, str, str, str]:
    if len(argv) != 6:
        print(
            "Usage: python backup_config.py "
            "<LS_URL> <LS_Username> <LS_Password> <Customer> <SFTP_Folder>"
        )
        sys.exit(1)
    _, base_url, username, password, customer, sftp_folder = argv
    return base_url, username, password, customer, sftp_folder


if __name__ == "__main__":
    _base_url, _username, _password, _customer, _sftp_folder = _parse_args(sys.argv)
    try:
        backup_config(_base_url, _username, _password, _customer, _sftp_folder)
    except (RuntimeError, KeyError, requests.RequestException) as err:
        print("Backup failed: %s", err)
        sys.exit(1)