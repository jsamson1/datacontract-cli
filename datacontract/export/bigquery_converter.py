import json
import logging
from typing import Dict, List

from datacontract.model.data_contract_specification import Model, Field, Server
from datacontract.model.exceptions import DataContractException


def to_bigquery_json(model_name: str, model_value: Model, server: Server) -> str:
    bigquery_table = to_bigquery_schema(model_name, model_value, server)
    return json.dumps(bigquery_table, indent=2)


def to_bigquery_schema(model_name: str, model_value: Model, server: Server) -> dict:
    return {
        "kind": "bigquery#table",
        "tableReference": {"datasetId": server.dataset, "projectId": server.project, "tableId": model_name},
        "description": model_value.description,
        "schema": {"fields": to_fields_array(model_value.fields)},
    }


def to_fields_array(fields: Dict[str, Field]) -> List[Dict[str, Field]]:
    bq_fields = []
    for field_name, field in fields.items():
        bq_fields.append(to_field(field_name, field))

    return bq_fields


def to_field(field_name: str, field: Field) -> dict:
    bq_type = map_type_to_bigquery(field.type, field_name)
    bq_field = {
        "name": field_name,
        "type": bq_type,
        "mode": "REQUIRED" if field.required else "NULLABLE",
        "description": field.description,
    }

    # handle arrays
    if field.type == "array":
        bq_field["mode"] = "REPEATED"
        if field.items.type == "object":
            # in case the array type is a complex object, we want to copy all its fields
            bq_field["fields"] = to_fields_array(field.items.fields)
        else:
            # otherwise we make up a structure that gets us a single field of the specified type
            bq_field["fields"] = to_fields_array(
                {f"{field_name}_1": Field(type=field.items.type, required=False, description="")}
            )
    # all of these can carry other fields
    elif bq_type.lower() in ["record", "struct"]:
        bq_field["fields"] = to_fields_array(field.fields)

    # strings can have a maxlength
    if bq_type.lower() == "string":
        bq_field["maxLength"] = field.maxLength

    # number types have precision and scale
    if bq_type.lower() in ["numeric", "bignumeric"]:
        bq_field["precision"] = field.precision
        bq_field["scale"] = field.scale

    return bq_field


def map_type_to_bigquery(type_str: str, field_name: str) -> str:
    logger = logging.getLogger(__name__)
    if type_str.lower() in ["string", "varchar", "text"]:
        return "STRING"
    elif type_str == "bytes":
        return "BYTES"
    elif type_str.lower() in ["int", "integer"]:
        return "INTEGER"
    elif type_str.lower() in ["long", "bigint"]:
        return "INT64"
    elif type_str == "float":
        return "FLOAT"
    elif type_str == "boolean":
        return "BOOL"
    elif type_str.lower() in ["timestamp", "timestamp_tz"]:
        return "TIMESTAMP"
    elif type_str == "date":
        return "DATE"
    elif type_str == "timestamp_ntz":
        return "TIME"
    elif type_str.lower() in ["number", "decimal", "numeric"]:
        return "NUMERIC"
    elif type_str == "double":
        return "BIGNUMERIC"
    elif type_str.lower() in ["object", "record", "array"]:
        return "RECORD"
    elif type_str == "struct":
        return "STRUCT"
    elif type_str == "null":
        logger.info(
            f"Can't properly map {field_name} to bigquery Schema, as 'null' is not supported as a type. Mapping it to STRING."
        )
        return "STRING"
    else:
        raise DataContractException(
            type="schema",
            result="failed",
            name="Map datacontract type to bigquery data type",
            reason=f"Unsupported type {type_str} in data contract definition.",
            engine="datacontract",
        )
