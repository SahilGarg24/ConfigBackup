import os
import requests
import sys
import urllib3
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from pathlib import Path

# Suppress SSL warnings globally since verify=False is used throughout
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def filter_files(path: str) -> list[str]:
    """Return all ZIP files whose name contains 'configbackup_' (case-insensitive)."""
    filtered = []
    for root, _dirs, files in os.walk(path):
        for file in files:
            # FIX: compare lower-cased search string against lower-cased filename
            if file.lower().endswith(".zip") and "configbackup_" in file.lower():
                filtered.append(os.path.join(root, file))
    return filtered


def upload_file(
    file_path: str,
    tenant_url: str,
    tenant_name: str,
    base_path: str,
    root_path: str,
    auth_headers: dict,
) -> None:
    download_root = Path(root_path) / "Config_Processing" / "Data" / "Download"

    # FIX: use pathlib so the relative path uses the correct OS separator
    later_path = Path(file_path).relative_to(download_root)
    folder_name = later_path.parts[0]           # first component, OS-agnostic

    # FIX: quote the boolean value properly as the string "false"
    file_url = (
        f"{tenant_url}/api/v3/integration/{tenant_name}/DataLakefiles/upload"
        f"?filePath={base_path}/{folder_name}&overrideFile=false"
    )
    try:
        with open(file_path, "rb") as f:
            response = requests.post(
                file_url, headers=auth_headers, files={"file": f}, verify=False
            )
        print(f"File Moved: {later_path}; {response.text}")
    except Exception as e:
        print(f"Error while uploading file: {e}")


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

    print(f"Authenticated — TenantId={tenant_id}  TenantName={tenant_name}")
    return token, tenant_id, tenant_name


def uploadFiles(
    baseurl: str,
    _username: str,
    _password: str,
    _basepath: str,
    root_path: str,
) -> None:
    """Discover ZIP backups and upload them concurrently to the cloud location."""
    base_url = baseurl.rstrip("/")
    if base_url.endswith("/api"):
        base_url = base_url[:-4]
    if not base_url.startswith("https://"):
        base_url = "https://" + base_url

    token, _tenant_id, tenant_name = _authenticate(base_url, _username, _password)
    auth_headers = _auth_headers(token)

    processed_path = str(Path(root_path) / "Config_Processing" / "Data" / "Download")
    filtered_zips = filter_files(processed_path)
    print(filtered_zips)

    with ThreadPoolExecutor(max_workers=25) as executor:
        results = list(
            executor.map(
                upload_file,
                filtered_zips,
                repeat(base_url),
                repeat(tenant_name),
                repeat(_basepath),
                repeat(root_path),
                repeat(auth_headers),
            )
        )
    print(f"Upload File Results: {results}")


def _parse_args(argv: list[str]) -> tuple[str, str, str, str, str]:
    if len(argv) != 6:
        print(
            "Usage: python upload_files.py "
            "<LS_URL> <LS_Username> <LS_Password> <Basepath> <Root_Path>"
        )
        sys.exit(1)
    _, _baseurl, _username, _password, _basepath, root_path = argv
    return _baseurl, _username, _password, _basepath, root_path


if __name__ == "__main__":
    _baseurl, _username, _password, _basepath, root_path = _parse_args(sys.argv)
    root_path = root_path.rstrip("\\").replace("\\", "/")

    try:
        uploadFiles(_baseurl, _username, _password, _basepath, root_path)
    except (RuntimeError, KeyError) as err:
        print(f"Backup failed: {err}")
        sys.exit(1)

    sys.exit(0)