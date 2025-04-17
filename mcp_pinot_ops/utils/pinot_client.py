import logging
from typing import Any
import pandas as pd
import requests
import mcp.types as types
from pinotdb import connect
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger("pinot_mcp_claude")

# Get configuration from environment variables
PINOT_CONTROLLER_URL = os.getenv("PINOT_CONTROLLER_URL")
PINOT_BROKER_HOST = os.getenv("PINOT_BROKER_HOST")
PINOT_BROKER_PORT = int(os.getenv("PINOT_BROKER_PORT", "443"))
PINOT_BROKER_SCHEME = os.getenv("PINOT_BROKER_SCHEME", "https")
PINOT_USERNAME = os.getenv("PINOT_USERNAME")
PINOT_PASSWORD = os.getenv("PINOT_PASSWORD")
PINOT_USE_MSQE = os.getenv("PINOT_USE_MSQE", "false").lower() == "true"
PINOT_TOKEN = os.getenv("PINOT_TOKEN", "")

HEADERS = {
    "accept": "application/json",
}
if PINOT_TOKEN:
    HEADERS["Authorization"] = PINOT_TOKEN

conn = connect(
    host=PINOT_BROKER_HOST,
    port=PINOT_BROKER_PORT,
    path="/query/sql",
    scheme=PINOT_BROKER_SCHEME,
    username=PINOT_USERNAME,
    password=PINOT_PASSWORD,
    use_multistage_engine=PINOT_USE_MSQE,
)


class Pinot:
    def __init__(self):
        self.insights: list[str] = []

    def _execute_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        logger.debug(f"Executing query: {query}")
        curs = conn.cursor()
        curs.execute(query)
        df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
        return df.to_dict(orient="records")

    def _get_tables(self, params: dict[str, Any] | None = None) -> list[str]:
        url = f"{PINOT_CONTROLLER_URL}/tables"
        return requests.get(url, headers=HEADERS).json()["tables"]

    def _get_table_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}/size"
        return requests.get(url, headers=HEADERS).json()

    def _get_segment__metadata_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/segments/{tableName}/metadata"
        return requests.get(url, headers=HEADERS).json()

    def _get_segments(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/segments/{tableName}"
        return requests.get(url, headers=HEADERS).json()

    def _get_index_column_detail(self, tableName: str, segmentName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        for type_suffix in ["REALTIME", "OFFLINE"]:
            url = f"{PINOT_CONTROLLER_URL}/segments/{tableName}_{type_suffix}/{segmentName}/metadata?columns=*"
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                return response.json()
        raise ValueError("Index column detail not found")

    def _get_tableconfig_schema_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tableConfigs/{tableName}"
        return requests.get(url, headers=HEADERS).json()

    def _pause_consumption(self, tableName: str, comment: str | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}/pauseConsumption"
        params = {}
        if comment:
            params["comment"] = comment
        response = requests.post(url, headers=HEADERS, params=params)
        response.raise_for_status() # Raise an exception for bad status codes
        # Check if response body is empty or just whitespace
        if not response.text or response.text.isspace():
            return {"status": "success", "message": "Pause request sent successfully."}
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            # Handle cases where response is not JSON but status code is OK (e.g., 200 OK with plain text)
            return {"status": "success", "response_body": response.text}

    def _resume_consumption(self, tableName: str, comment: str | None = None, consumeFrom: str | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}/resumeConsumption"
        params = {}
        if comment:
            params["comment"] = comment
        if consumeFrom:
            params["consumeFrom"] = consumeFrom
        response = requests.post(url, headers=HEADERS, params=params)
        response.raise_for_status()
        if not response.text or response.text.isspace():
            return {"status": "success", "message": "Resume request sent successfully."}
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {"status": "success", "response_body": response.text}

    def _force_commit(self, tableName: str, partitions: str | None = None, segments: str | None = None,
                      batchSize: int | None = None, batchStatusCheckIntervalSec: int | None = None,
                      batchStatusCheckTimeoutSec: int | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}/forceCommit"
        params = {}
        if partitions:
            params["partitions"] = partitions
        if segments:
            params["segments"] = segments
        if batchSize is not None:
            params["batchSize"] = batchSize
        if batchStatusCheckIntervalSec is not None:
            params["batchStatusCheckIntervalSec"] = batchStatusCheckIntervalSec
        if batchStatusCheckTimeoutSec is not None:
            params["batchStatusCheckTimeoutSec"] = batchStatusCheckTimeoutSec

        response = requests.post(url, headers=HEADERS, params=params)
        response.raise_for_status()
        if not response.text or response.text.isspace():
            # The API spec indicates a response schema, but let's handle empty responses gracefully
            return {"status": "success", "message": "Force commit request submitted."}
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
             return {"status": "success", "response_body": response.text}

    def _get_pause_status(self, tableName: str) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}/pauseStatus"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        if not response.text or response.text.isspace():
            return {"status": "success", "message": "Pause status retrieved, but response was empty."}
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
             return {"status": "success", "response_body": response.text}

    def _get_consuming_segments_info(self, tableName: str) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}/consumingSegmentsInfo"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            # The API spec indicates a response schema, so this shouldn't usually happen on success
            return {"status": "error", "message": "Failed to decode JSON response", "response_body": response.text}

    def _reload_table_segments(self, tableName: str, tableType: str | None = None, forceDownload: bool = False) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/segments/{tableName}/reload"
        params = {
            "forceDownload": str(forceDownload).lower()
        }
        if tableType:
            params["type"] = tableType

        response = requests.post(url, headers=HEADERS, params=params)
        response.raise_for_status()
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {"status": "success", "message": "Reload request sent.", "response_body": response.text}

    def _rebalance_table(self, tableName: str, tableType: str,
                         dryRun: bool = False, reassignInstances: bool = True, includeConsuming: bool = True,
                         bootstrap: bool = False, downtime: bool = False, minAvailableReplicas: int = -1,
                         **kwargs) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}/rebalance"
        params = {
            "type": tableType,
            "dryRun": str(dryRun).lower(),
            "reassignInstances": str(reassignInstances).lower(),
            "includeConsuming": str(includeConsuming).lower(),
            "bootstrap": str(bootstrap).lower(),
            "downtime": str(downtime).lower(),
            "minAvailableReplicas": minAvailableReplicas
        }
        # Add any other optional params passed via kwargs
        for k, v in kwargs.items():
            if v is not None:
                 # Convert boolean values to lowercase strings for Pinot API
                if isinstance(v, bool):
                    params[k] = str(v).lower()
                else:
                    params[k] = v

        response = requests.post(url, headers=HEADERS, params=params)
        response.raise_for_status()
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {"status": "success", "message": "Rebalance request sent.", "response_body": response.text}

    def _reset_table_segments(self, tableNameWithType: str, errorSegmentsOnly: bool = False) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/segments/{tableNameWithType}/reset"
        params = {
            "errorSegmentsOnly": str(errorSegmentsOnly).lower()
        }
        response = requests.post(url, headers=HEADERS, params=params)
        response.raise_for_status()
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {"status": "success", "message": "Reset segments request sent.", "response_body": response.text}

    def _create_schema(self, schemaJson: str, override: bool = True, force: bool = False) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/schemas"
        params = {
            "override": str(override).lower(),
            "force": str(force).lower()
        }
        # The API expects multipart/form-data or application/json based on swagger, but examples use form data.
        # Let's try sending JSON directly first, as it's simpler. Check swagger again - it lists FormDataMultiPart.
        # Trying with JSON body. If this fails, might need multipart handling.
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/json'
        response = requests.post(url, headers=headers, params=params, data=schemaJson)

        # If JSON fails, try multipart (more complex to construct)
        # if response.status_code >= 400:
        #    files = {'file': ('schema.json', schemaJson, 'application/json')}
        #    response = requests.post(url, headers=HEADERS, params=params, files=files)

        response.raise_for_status()
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
             # Handle cases like 200 OK with non-JSON success message
             return {"status": "success", "message": "Schema creation request processed.", "response_body": response.text}

    def _update_schema(self, schemaName: str, schemaJson: str, reload: bool = False, force: bool = False) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/schemas/{schemaName}"
        params = {
            "reload": str(reload).lower(),
            "force": str(force).lower()
        }
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/json'
        response = requests.put(url, headers=headers, params=params, data=schemaJson)
        response.raise_for_status()
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {"status": "success", "message": "Schema update request processed.", "response_body": response.text}

    def _create_table_config(self, tableConfigJson: str, validationTypesToSkip: str | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables"
        params = {}
        if validationTypesToSkip:
            params["validationTypesToSkip"] = validationTypesToSkip
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/json'
        response = requests.post(url, headers=headers, params=params, data=tableConfigJson)
        response.raise_for_status()
        return response.json() # Expects JSON response based on swagger

    def _update_table_config(self, tableName: str, tableConfigJson: str, validationTypesToSkip: str | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}"
        params = {}
        if validationTypesToSkip:
            params["validationTypesToSkip"] = validationTypesToSkip
        headers = HEADERS.copy()
        headers['Content-Type'] = 'application/json'
        response = requests.put(url, headers=headers, params=params, data=tableConfigJson)
        response.raise_for_status()
        return response.json() # Expects JSON response based on swagger

    def _get_table_config(self, tableName: str, tableType: str | None = None) -> dict[str, Any]:
        """Gets the table config for a specific table. Use tableType for REALTIME/OFFLINE specificity if needed, but /tables/{tableName} GET might return combined.
           Checking swagger: GET /tables/{tableName} returns config based on 'type' query param or combined.
           Let's assume we get the config we need to PUT back.
        """
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}"
        params = {}
        if tableType:
            params["type"] = tableType # Query param for GET

        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        # The response for GET /tables/{tableName} returns a structure like {"OFFLINE": {config...}, "REALTIME": {config...}}
        # or just {config...} if type is specified. The PUT /tables/{tableName} expects just the config object.
        # We need to handle this. Let's return the raw response for now, the caller needs to extract the right part.
        # Or, we can try to be smarter if tableType is provided.
        raw_response = response.json()
        if tableType and tableType.upper() in raw_response:
            return raw_response[tableType.upper()] # Return specific type config
        elif not tableType and ("OFFLINE" in raw_response or "REALTIME" in raw_response):
            # If type not specified, return the whole structure if it contains types
            # Or maybe just return the first one found? Let's return the whole thing.
             return raw_response
        else:
             # Assume it's the direct config if no types are keys
             return raw_response

    def _add_index(self, tableName: str, indexType: str, columns: list[str],
                     tableType: str | None = None, triggerReload: bool = True) -> dict[str, Any]:
        """Adds an index configuration to a table and optionally reloads.

        Args:
            tableName: The name of the table (without type suffix).
            indexType: The type of index (e.g., 'inverted', 'range', 'json').
            columns: List of column names to apply the index to.
            tableType: Specify 'OFFLINE' or 'REALTIME' if the table has both types.
            triggerReload: Whether to reload segments after updating the config.

        Returns:
            A status dictionary.
        """
        import json

        try:
            # 1. Get current table config
            # We need the specific config (OFFLINE or REALTIME) or the combined one if only one type exists
            # The _get_table_config handles fetching the specific type if provided, or the combined dict otherwise.
            current_config_response = self._get_table_config(tableName, tableType)

            # Determine which config object to modify
            if tableType:
                config_to_modify = current_config_response # _get_table_config returns the specific type
            elif "OFFLINE" in current_config_response:
                config_to_modify = current_config_response["OFFLINE"]
                if tableType is None: tableType = "OFFLINE" # Default to modifying OFFLINE if type unspecified
            elif "REALTIME" in current_config_response:
                config_to_modify = current_config_response["REALTIME"]
                if tableType is None: tableType = "REALTIME" # Default to REALTIME if only that exists
            else:
                # Assume it's a direct config object (e.g., only one type exists and type wasn't specified in GET)
                config_to_modify = current_config_response
                # We still need to know the type for reload later, try to infer or require it?
                # Let's require tableType if the GET response was ambiguous (not directly a config object)
                if not isinstance(config_to_modify.get("tableName"), str):
                     raise ValueError("Could not determine table config structure. Please specify tableType (OFFLINE or REALTIME).")
                if tableType is None:
                    # Infer type from tableName if possible (heuristic)
                    if config_to_modify.get("tableType") == "REALTIME":
                        tableType = "REALTIME"
                    else:
                        tableType = "OFFLINE" # Default assumption

            if not config_to_modify or "tableIndexConfig" not in config_to_modify:
                 # Initialize tableIndexConfig if it doesn't exist
                 config_to_modify["tableIndexConfig"] = {}
            elif config_to_modify["tableIndexConfig"] is None:
                 config_to_modify["tableIndexConfig"] = {}

            index_config = config_to_modify["tableIndexConfig"]

            # Mapping from tool indexType to Pinot config key
            index_key_map = {
                "inverted": "invertedIndexColumns",
                "range": "rangeIndexColumns",
                "text": "textIndexColumns",
                "json": "jsonIndexColumns",
                "bloom": "bloomFilterColumns",
                "fst": "fstIndexColumns",
                "sorted": "sortedColumn" # Note: Only one sorted column allowed usually
            }

            if indexType not in index_key_map:
                raise ValueError(f"Unsupported indexType: {indexType}")

            config_key = index_key_map[indexType]

            # 2. Modify the config
            if config_key not in index_config or index_config[config_key] is None:
                index_config[config_key] = []

            # Add columns, ensuring no duplicates
            existing_columns = set(index_config[config_key])
            for col in columns:
                existing_columns.add(col)

            # Special handling for sortedColumn (expects single value in list)
            if config_key == "sortedColumn":
                 if len(existing_columns) > 1:
                      logger.warning(f"Request to add multiple sorted columns ({list(existing_columns)}). Pinot typically supports only one. Setting to the first requested column: {columns[0]}")
                      index_config[config_key] = [columns[0]]
                 elif len(existing_columns) == 1:
                      index_config[config_key] = list(existing_columns)
                 else: # No columns requested/left
                     if config_key in index_config:
                        del index_config[config_key]
            else:
                index_config[config_key] = sorted(list(existing_columns))

            # 3. Update the table config via PUT
            # The PUT /tables/{tableName} expects the raw config object as the body
            update_response = self._update_table_config(tableName, json.dumps(config_to_modify))
            logger.info(f"Table config update response for {tableName}: {update_response}")

            # 4. Optionally trigger reload
            reload_status = "Not triggered."
            if triggerReload:
                logger.info(f"Triggering reload for table {tableName} (type: {tableType})")
                # Ensure tableType is determined for reload API
                if not tableType:
                     raise ValueError("Table type (OFFLINE/REALTIME) could not be determined for reload. Please specify.")
                reload_response = self._reload_table_segments(tableName, tableType=tableType)
                reload_status = f"Reload triggered: {reload_response}"
                logger.info(reload_status)

            return {"status": "success", "message": f"Index '{indexType}' added to columns {columns} for table {tableName}. Config updated. {reload_status}"}

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP Error adding index for table {tableName}: {e}")
            return {"status": "error", "message": f"HTTP Error: {e}"}
        except ValueError as e:
             logger.error(f"Value Error adding index for table {tableName}: {e}")
             return {"status": "error", "message": f"Value Error: {e}"}
        except Exception as e:
            logger.error(f"Unexpected error adding index for table {tableName}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"status": "error", "message": f"An unexpected error occurred: {e}"}

    def _add_star_tree_index(self, tableName: str, dimensionsSplitOrder: list[str],
                               functionColumnPairs: list[str] | None = None,
                               aggregationConfigsJson: str | None = None,
                               skipStarNodeCreationForDimensions: list[str] | None = None,
                               maxLeafRecords: int = 10000,
                               tableType: str | None = None,
                               triggerReload: bool = True) -> dict[str, Any]:
        """Adds a Star-Tree index configuration to a table and optionally reloads.

        Args:
            tableName: Name of the table.
            dimensionsSplitOrder: List of dimensions defining tree structure.
            functionColumnPairs: List like ["SUM__colA", "COUNT__*"]. Use this OR aggregationConfigsJson.
            aggregationConfigsJson: JSON string representing the 'aggregationConfigs' list.
            skipStarNodeCreationForDimensions: Optional list of dimensions to skip Star-node creation.
            maxLeafRecords: Optional threshold for splitting nodes.
            tableType: Specify 'OFFLINE' or 'REALTIME' if needed.
            triggerReload: Whether to reload segments after update.

        Returns:
            A status dictionary.
        """
        import json

        if functionColumnPairs and aggregationConfigsJson:
            raise ValueError("Provide either functionColumnPairs or aggregationConfigsJson, not both.")

        try:
            # 1. Get current table config
            current_config_response = self._get_table_config(tableName, tableType)

            # Determine config object to modify (similar logic as _add_index)
            config_to_modify = None
            original_table_type = tableType # Keep track for reload
            if tableType:
                config_to_modify = current_config_response
            elif isinstance(current_config_response.get("OFFLINE"), dict):
                config_to_modify = current_config_response["OFFLINE"]
                if original_table_type is None: original_table_type = "OFFLINE"
            elif isinstance(current_config_response.get("REALTIME"), dict):
                config_to_modify = current_config_response["REALTIME"]
                if original_table_type is None: original_table_type = "REALTIME"
            elif isinstance(current_config_response.get("tableName"), str):
                config_to_modify = current_config_response
                if original_table_type is None: original_table_type = config_to_modify.get("tableType", "OFFLINE")
            else:
                 raise ValueError("Could not determine table config structure. Please specify tableType (OFFLINE or REALTIME).")

            # Ensure tableIndexConfig exists
            if "tableIndexConfig" not in config_to_modify or config_to_modify["tableIndexConfig"] is None:
                 config_to_modify["tableIndexConfig"] = {}
            index_config = config_to_modify["tableIndexConfig"]

            # Ensure starTreeIndexConfigs list exists
            if "starTreeIndexConfigs" not in index_config or index_config["starTreeIndexConfigs"] is None:
                index_config["starTreeIndexConfigs"] = []

            # 2. Construct the new star-tree config object
            new_star_tree_config = {
                "dimensionsSplitOrder": dimensionsSplitOrder,
                "maxLeafRecords": maxLeafRecords
            }
            if skipStarNodeCreationForDimensions:
                new_star_tree_config["skipStarNodeCreationForDimensions"] = skipStarNodeCreationForDimensions

            # Add aggregations
            if aggregationConfigsJson:
                try:
                    new_star_tree_config["aggregationConfigs"] = json.loads(aggregationConfigsJson)
                except json.JSONDecodeError as json_err:
                    raise ValueError(f"Invalid JSON provided for aggregationConfigsJson: {json_err}")
            elif functionColumnPairs:
                 new_star_tree_config["functionColumnPairs"] = functionColumnPairs
            # else: No aggregations specified, which might be valid for some use cases or defaults?

            # 3. Append to the list
            index_config["starTreeIndexConfigs"].append(new_star_tree_config)

            # 4. Update the table config via PUT
            update_response = self._update_table_config(tableName, json.dumps(config_to_modify))
            logger.info(f"Table config update response for {tableName} (Star-Tree): {update_response}")

            # 5. Optionally trigger reload
            reload_status = "Not triggered. Note: Star-Tree index creation often requires segment regeneration."
            if triggerReload:
                logger.info(f"Triggering reload for table {tableName} (type: {original_table_type}) after Star-Tree config update.")
                if not original_table_type:
                     raise ValueError("Table type (OFFLINE/REALTIME) could not be determined for reload. Please specify.")
                try:
                    reload_response = self._reload_table_segments(tableName, tableType=original_table_type)
                    reload_status = f"Reload triggered: {reload_response}. Note: Star-Tree index creation often requires segment regeneration."
                    logger.info(reload_status)
                except Exception as reload_err:
                    reload_status = f"Config updated, but failed to trigger reload: {reload_err}"
                    logger.error(reload_status)

            return {"status": "success", "message": f"Star-Tree index config added to table {tableName}. {reload_status}"}

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP Error adding Star-Tree index for table {tableName}: {e}")
            return {"status": "error", "message": f"HTTP Error: {e}"}
        except ValueError as e:
             logger.error(f"Value Error adding Star-Tree index for table {tableName}: {e}")
             return {"status": "error", "message": f"Value Error: {e}"}
        except Exception as e:
            logger.error(f"Unexpected error adding Star-Tree index for table {tableName}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"status": "error", "message": f"An unexpected error occurred: {e}"}
