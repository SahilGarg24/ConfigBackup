import json
import sys, re
from datetime import datetime
import paramiko
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

file_path = "/opt/airflow/dags/CCM_Parameters.json"
with open(file_path, "r") as file:
    Parameters = json.load(file)

tenant_id     = Parameters["Tenant_ID"]
LS_URL        = Parameters["LS_URL"] + "/api"
userEmail     = Parameters["EtlUsername"]
submittedPassword = Parameters["EtlPassword"]
TenantName    = Parameters["TenantName"]
Customer      = Parameters["CustomerName"]
EnviName      = Parameters["EnvironmentName"]
sftp_folder_name = Parameters["SFTP_FolderName"]
sftpusername  = Parameters["SFTP_username"]
sftppassword  = Parameters["SFTP_Password"]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
AUTH_EXPIRY_SECONDS = 36000
BACKUP_ACTIVITY = "BackupTenantConfigActivity"
CHUNK_SIZE = 1048576  # 1 MiB
JSON_HEADERS = {"Content-Type": "application/json"}
SFTP_HOST = "sftpdcxprod.o9solutions.com"
SFTP_PORT = 2222


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sanitize_isoformat(created_at: str) -> str:
    s = created_at.replace("Z", "+00:00")
    # Pad OR truncate fractional seconds to exactly 6 digits
    s = re.sub(
        r'\.(\d+)([+-]\d{2}:\d{2}|$)',
        lambda m: f".{m.group(1)[:6].ljust(6, '0')}{m.group(2)}",
        s
    )
    return s


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Basic {token}"}


def _raise_for_status(response: requests.Response, context: str) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"{context} failed — HTTP {response.status_code}: {response.text}"
        ) from exc


def _get_workflow_id_from_last_actions(base_url: str, tid: str, token: str) -> str | None:
    """Try to find the backup workflow ID from the last-actions endpoint."""
    response = requests.get(
        f"{base_url}/workflow/tenant/{tid}/lastactions",
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


def _get_workflow_id_from_backups(base_url: str, tenant_name: str, token: str) -> str | None:
    """Find the most-recent backup workflow ID from the backups list endpoint."""
    response = requests.get(
        f"{base_url}/tenants/{tenant_name}/backups",
        headers=_auth_headers(token),
        verify=False,
    )
    _raise_for_status(response, "Backups list fetch")

    latest_dt = None
    workflow_id = None

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
# Callable for PythonOperator
# ---------------------------------------------------------------------------
def backup_config() -> None:
    # 1. Authenticate
    auth_response = requests.post(
        f"{LS_URL}/framework/auth/login",
        json={"userEmail": userEmail, "submittedPassword": submittedPassword, "expirySeconds": AUTH_EXPIRY_SECONDS},
        headers=JSON_HEADERS,
        verify=False,
    )
    _raise_for_status(auth_response, "Authentication")

    token = auth_response.json()["AuthToken"]
    tenant_name = TenantName.split("-")[0]
    print(f"Authenticated — TenantId: {tenant_id}  TenantName: {tenant_name}")

    # 2. Resolve workflow ID — try fast path first, then fall back
    workflow_id = _get_workflow_id_from_last_actions(LS_URL, tenant_id, token)
    if not workflow_id:
        print("Last-actions did not yield a workflow ID; trying backups list...")
        workflow_id = _get_workflow_id_from_backups(LS_URL, tenant_name, token)

    if not workflow_id:
        raise RuntimeError("Could not determine the backup workflow ID.")
    print(f"Resolved workflow_id: {workflow_id}")

    # 3. Fetch the pre-signed download URL
    url_response = requests.get(
        f"{LS_URL}/tenants/{tenant_name}/backups/{workflow_id}/activity/{BACKUP_ACTIVITY}",
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
    filename = f"ConfigBackup_{Customer}_{sftp_folder_name}.zip"
    remote_path = f"/{sftp_folder_name}/ToCCM/{filename}"

    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
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

    print(f"Config backup uploaded successfully to {remote_path}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
params = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "o9AF_tenant_id": tenant_id,
    "o9AF_tenant_name": TenantName.split("-")[0],
    "o9AF_environment_name": EnviName.split("-")[0],
    "o9AF_webapi_url": LS_URL,
}

with DAG(
    "CCM_ConfigBackup",
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    schedule_interval=None,
    params=params,
    catchup=False,
) as dag:
    start = DummyOperator(task_id="Start", dag=dag)

    tenant_backup = PythonOperator(
        task_id="Fetch_TenantBackupConfig",
        python_callable=backup_config,
        trigger_rule="none_failed",
    )

    end = DummyOperator(task_id="end")

    start >> tenant_backup >> end