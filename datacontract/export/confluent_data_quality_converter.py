import json

from datacontract.export.exporter import Exporter, _check_models_for_export


class ConfluentDataQualityExporter(Exporter):
    def export(self, data_contract, model, server, sql_server_type, export_args) -> dict:
        model_value = _check_models_for_export(data_contract, model, self.export_format)[1]
        return to_ruleset_json(model_value)


def to_ruleset_json(model) -> str:
    rules_json = {}
    rules_json["ruleSet"] = {"domainRules": to_fields(model.fields)}
    return json.dumps(rules_json, indent=2, sort_keys=False)


def to_fields(fields):
    result = []
    for field_name, field in fields.items():
        if field.config and "dq_rule" in field.config:
            result.append(to_rule(field, field_name))
    return result


def to_rule(field, field_name):
    quality_rule = {"name": "check_" + field_name,
                        "kind": "CONDITION",
                        "mode": "WRITE"
                    }
    if field.required == False:
        quality_rule["type"] = "CEL_FIELD"
        if field.type in ["string", "varchar", "text"]:
            quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'STRING' ; " + field.config["dq_rule"]
        elif field.type in ["integer", "int", "date"]:
            quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'INT' ; " + field.config["dq_rule"]
        elif field.type in ["long", "bigint", "timestamp", "timestamp_tz", "timestamp_ntz", "time"]:
            quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'LONG' ; " + field.config["dq_rule"]
        elif field.type == "float":
            quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'FLOAT' ; " + field.config["dq_rule"]
        elif field.type == "double":
            quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'DOUBLE' ; " + field.config["dq_rule"]
        elif field.type in ["boolean"]:
            quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'BOOLEAN' ; " + field.config["dq_rule"]
        else:
            quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'BYTES' ; " + field.config["dq_rule"]  
    else:
        quality_rule["type"] = "CEL"
        quality_rule["expr"] = field.config["dq_rule"]

    if "dq_dlq_topic" in field.config:
        quality_rule["onFailure"] = "DLQ"
        quality_rule["params"] = { "dlq.topic": field.config["dq_dlq_topic"] }
    else:
        quality_rule["onFailure"] = "ERROR"

    return quality_rule
