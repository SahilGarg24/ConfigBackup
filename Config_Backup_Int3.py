import sys
from datetime import datetime
from logging import getLogger
logger = getLogger('o9_logger')
import paramiko, re
import requests

sftp_host = "sftpdcxprod.o9solutions.com"
sftp_port = 2222
sftp_username = '{{ccm_params.SFTP_Username}}'
sftp_password = '{{ccm_params.SFTP_Password}}'

tenant_url = '{{ccm_params.Tenant_URL}}'
ssh_username = '{{ccm_params.ETL_Username}}'
ssh_password = '{{ccm_params.ETL_Password}}'
customer_name = '{{ccm_params.Customer_Name}}'
sftp_folder_name = '{{ccm_params.SFTP_FolderName}}'

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CHUNK_SIZE = 1048576
JSON_HEADERS = {"Content-Type": "application/json"}


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Basic {token}"}


def _raise_for_status(response: requests.Response, context: str) -> None:
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

def _get_workflow_id_from_last_actions(
    base_url: str, tenant_id: str, token: str
) -> str | None:
    """Try to find the backup workflow ID from the last-actions endpoint."""
    response = requests.get(
        f"{base_url}/api/workflow/tenant/{tenant_id}/lastactions",
        headers=_auth_headers(token),
        verify=False,
    )
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
    base_url: str, tenant_name: str, token: str
) -> str | None:
    """Find the most-recent backup workflow ID from the backups list endpoint."""
    response = requests.get(
        f"{base_url}/api/tenants/{tenant_name}/backups",
        headers=_auth_headers(token),
        verify=False,
    )
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


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------
def backup_config(
    base_url: str,
    username: str,
    password: str,
    customer: str,
    sftp_folder: str,
    sftphost: str,
    sftpport: int,
    sftpusername: str,
    sftppassword: str,
) -> None:

    # 1. Authenticate
    logger.info(f"starting tenant authentication")
    auth_response = requests.post(
        f"{base_url}/api/framework/auth/login",
        json={"userEmail": username, "submittedPassword": password, "expirySeconds": 36000},
        headers=JSON_HEADERS,
        verify=False,
    )
    _raise_for_status(auth_response, "Authentication")

    body = auth_response.json()
    token = body["AuthToken"]
    tenant_id = str(body["TenantId"])
    tenant_name = str(body["TenantName"])
    logger.info(f"Authenticated — TenantId: {tenant_id}  TenantName: {tenant_name}")

    # 2. Resolve workflow ID — try fast path first, then fall back
    workflow_id = _get_workflow_id_from_last_actions(base_url, tenant_id, token)
    if not workflow_id:
        logger.info("Last-actions did not yield a workflow ID; trying backups list...")
        workflow_id = _get_workflow_id_from_backups(base_url, tenant_name, token)

    if not workflow_id:
        raise RuntimeError("Could not determine the backup workflow ID.")
    logger.info(f"Resolved workflow_id: {workflow_id}")

    # 3. Fetch the pre-signed download URL
    url_response = requests.get(
        f"{base_url}/api/tenants/{tenant_name}/backups/{workflow_id}/activity/BackupTenantConfigActivity",
        headers=_auth_headers(token),
        verify=False,
    )
    _raise_for_status(url_response, "Download-URL fetch")

    download_url = url_response.json()
    if not download_url:
        raise RuntimeError("Backup activity returned an empty download URL.")

    # 4. Open streaming download
    download_response = requests.get(
        download_url, allow_redirects=True, verify=False, stream=True, timeout=300
    )
    _raise_for_status(download_response, "ZIP download")

    file_size = int(download_response.headers.get("content-length", 0))

    # 5. Stream directly to SFTP
    filename = f"ConfigBackup_{customer}_{sftp_folder}.zip"
    remote_path = f"/{sftp_folder}/ToCCM/{filename}"

    transport = paramiko.Transport((sftphost, sftpport))
    try:
        transport.connect(username=sftpusername, password=sftppassword)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            uploaded = 0
            with sftp.file(remote_path, "wb") as remote_file:
                for chunk in download_response.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    remote_file.write(chunk)
                    uploaded += len(chunk)
                    if file_size:
                        sys.stdout.write(f"\rUploading: {(uploaded / file_size) * 100:.1f}%")
                        sys.stdout.flush()
            if file_size:
                sys.stdout.write("\n")
        finally:
            sftp.close()
    finally:
        transport.close()

    logger.info(f"Config backup uploaded successfully to {remote_path}")


if __name__ == "__main__":
    logger.info(f"Starting the Backup Extraction Process")
    backup_config(
        tenant_url, ssh_username, ssh_password, customer_name, sftp_folder_name,
        sftp_host, sftp_port, sftp_username, sftp_password,
    )
    logger.info(f"Backup Extraction Successful, exiting.")