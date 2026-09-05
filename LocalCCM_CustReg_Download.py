import pandas as pd
import sys, requests
from logging import getLogger

logger = getLogger(__name__)

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


def create_dataframe_from_api(resp):
    """
    Parses metadata to create an empty DataFrame with correct column headers.
    """
    columns = []
    query_response_dataframe = pd.DataFrame()

    if resp.get("Meta") and resp.get("Data"):
        logger.info("The Query response is not empty. Parsing Metadata...")

        # Fetch Dimension Attributes
        logger.info("Fetching Dimension Attributes...")
        for column in resp["Meta"]:
            logger.info(f"Meta Column: {column}")
            if "DimensionName" in column and "AttributeName" in column:
                col_name = "[{0}].[{1}]".format(column["DimensionName"], column["AttributeName"])
                columns.append(col_name)

        # Fetch Measure Columns
        for column in resp["Meta"]:
            if "Name" in column and "MeasureColumnName" in column:
                columns.append(column["Name"])

        logger.info(f"Final Column List: {columns}")
        query_response_dataframe = pd.DataFrame(columns=columns)
        logger.info("Successfully created the empty Dataframe structure.")

    elif not resp.get("Meta") and not resp.get("Data"):
        logger.warning("The Query response is empty (No Data/Meta found).")
    else:
        logger.warning(
            "Response has unexpected structure. Meta: %s, Data: %s",
            resp.get("Meta"),
            resp.get("Data"),
        )

    return query_response_dataframe


def populate_rows(resp, response_data):
    """
    Parses the 'Data' section to fill the DataFrame rows.
    """
    logger.info("Starting to populate DataFrame rows...")
    try:
        rows_list = []
        if "Data" in resp:
            for row_data in resp["Data"]:
                row = []
                k = 0

                # Loop: Extract Dimension Members
                while "C{0}".format(k) in row_data:
                    cell = row_data["C{0}".format(k)]
                    if "Name" in cell:
                        row.append(cell["Name"])
                    elif "Value" in cell:
                        row.append(cell["Value"])
                    k += 1

                rows_list.append(row)

        if rows_list:
            new_rows = pd.DataFrame(rows_list, columns=response_data.columns)

            if response_data.empty:
                response_data = new_rows
                logger.info(f"Successfully populated DataFrame with {len(rows_list)} rows.")
            else:
                response_data = pd.concat([response_data, new_rows], ignore_index=True)
                logger.info(f"Successfully appended {len(rows_list)} rows.")

    except Exception as e:
        logger.error(f"Error populating rows: {e}")

    return response_data


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
    response = requests.post(url, json=payload)
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
def download_custreg(
    base_url: str,
    username: str,
    password: str,
    query: str,
    root_path: str,
) -> None:

    base_url = base_url.rstrip("/")
    if base_url.endswith("/api"):
        base_url = base_url[:-4]
    if not base_url.startswith("https://"):
        base_url = "https://" + base_url

    # 1. Authenticate.
    token, tenant_id, tenant_name = _authenticate(base_url, username, password)
    auth_headers = _auth_headers(token)
    json_payload = {"Query": query}

    # 2. Execute query.
    res = requests.post(
        url=f"{base_url}/api/ibplquery/{tenant_id}/ExecuteQueryJson",
        headers=auth_headers,
        json=json_payload,
    )
    _raise_for_status(res, "fire_query")

    # 3. Parse response into DataFrame.
    query_response_df = create_dataframe_from_api(res.json())
    query_response_df = populate_rows(res.json(), query_response_df)

    logger.info("Cust Reg DF Created: %d rows", len(query_response_df))

    # FIX: use .empty check — a DataFrame is never None, so `is not None` always passes
    if query_response_df.empty:
        logger.warning("Unable to save the CustRegDF — DataFrame is empty.")
        return

    # 4. Rename columns.
    query_response_df = query_response_df.rename(columns={
        "[o9 Customer].[Customer]": "Customer",
        "Assigned Config Build": "Build",
        "CCM Customer Credentials LS URL": "Tenant_URL",
        "CCM Customer Credentials SFTP Folder Name": "SFTP_Folder",
        "CCM Customer Credentials Tenant ID": "Tenant_ID",
        "CCM Customer Credentials Tenant Name": "Tenant_Name",
    })

    # FIX: validate all expected columns exist before selecting — catches silent rename failures
    expected_cols = ["Customer", "Build", "Tenant_URL", "SFTP_Folder", "Tenant_ID", "Tenant_Name"]
    missing = [c for c in expected_cols if c not in query_response_df.columns]
    if missing:
        raise ValueError(f"Missing columns after rename: {missing}")

    # 5. Save to CSV.
    output_path = f"{root_path}/Cust_Reg_DF.csv"
    query_response_df[expected_cols].to_csv(output_path, index=False)
    logger.info("CustReg DF saved: %s", output_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(argv: list[str]) -> tuple[str, str, str, str, str]:
    if len(argv) != 6:
        print(
            "Usage: python download_custreg.py "
            "<LS_URL> <LS_Username> <LS_Password> <Query> <Root_Path>"
        )
        sys.exit(1)
    _, base_url, username, password, query, root_path = argv
    root_path = root_path.rstrip("\\").replace("\\", "/")
    return base_url, username, password, query, root_path


if __name__ == "__main__":
    _base_url, _username, _password, _query, _root = _parse_args(sys.argv)
    try:
        download_custreg(_base_url, _username, _password, _query, _root)
    except (RuntimeError, KeyError, ValueError, requests.RequestException) as err:
        logger.error("CustRegSave failed: %s", err)
        sys.exit(1)

    sys.exit(0)