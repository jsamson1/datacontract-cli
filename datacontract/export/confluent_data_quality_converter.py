
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
        result.extend(to_rule(field, field_name))
    return result


def to_rule(field, field_name):
    ret_val = []
    if field.type in ["string", "varchar", "text"]:
        return text_rule(field, field_name)
    if field.type in ["integer", "int", "date", "long", "bigint", "timestamp", "timestamp_tz", "timestamp_ntz", "time"]:
        return digit_rule(field, field_name)
    if field.type in ["float", "double"]:
        return decimal_rule(field, field_name)
    return ret_val


def text_rule(field, field_name):
    ret_val = []
    if field.pattern != None:
        quality_rule = {"name": "check_" + field_name + "_pattern",
                    "kind": "CONDITION",
                    "mode": "WRITE"
                }
        if field.required == False:
            quality_rule["type"] = "CEL_FIELD"
            quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'STRING' ; value.matches(r\"" + field.pattern + "\")"
            
        else:
            quality_rule["type"] = "CEL"
            quality_rule["expr"] = "message." + field_name + ".matches(r\"" + field.pattern + "\")"
        quality_rule = set_dlq_config(quality_rule, field)
        ret_val.append(quality_rule)
    if field.minLength != None or field.maxLength != None:
        if field.minLength != None and field.maxLength != None:
            quality_rule = {"name": "check_" + field_name + "_length",
                        "kind": "CONDITION",
                        "mode": "WRITE"
                    }
            if field.required == False:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'STRING' ; size(value) >= " + str(field.minLength) + " && size(value) <= " + str(field.maxLength)
                
            else:
                quality_rule["type"] = "CEL"
                quality_rule["expr"] = "size(message." + field_name + ") >= " + str(field.minLength) + " && size(message." + field_name + ") <= " + str(field.maxLength)
        elif field.minLength != None:
            quality_rule = {"name": "check_" + field_name + "_length",
                        "kind": "CONDITION",
                        "mode": "WRITE"
                    }
            if field.required == False:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'STRING' ; size(value) >= " + str(field.minLength) 
                
            else:
                quality_rule["type"] = "CEL"
                quality_rule["expr"] = "size(message." + field_name + ") >= " + str(field.minLength)
        elif field.maxLength != None:
            quality_rule = {"name": "check_" + field_name + "_length",
                        "kind": "CONDITION",
                        "mode": "WRITE"
                    }
            if field.required == False:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'STRING' ; size(value) <= " + str(field.maxLength) 
                
            else:
                quality_rule["type"] = "CEL"
                quality_rule["expr"] = "size(message." + field_name + ") <= " + str(field.maxLength)
        quality_rule = set_dlq_config(quality_rule, field)
        ret_val.append(quality_rule)
    return ret_val


def digit_rule(field, field_name):
    ret_val = []
    if field.precision != None:
        quality_rule = {"name": "check_" + field_name + "_precision",
                    "kind": "CONDITION",
                    "mode": "WRITE"
                }
        if field.required == False:
            if field.type in ["integer", "int", "date"]:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'INT' ; size(string(value)) <= " + str(field.precision) 
            else:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'LONG' ; size(string(value)) <= " + str(field.precision)
        else:
            quality_rule["type"] = "CEL"
            quality_rule["expr"] = "size(string(message." + field_name + ")) <= " + str(field.precision)
        quality_rule = set_dlq_config(quality_rule, field)
        ret_val.append(quality_rule)
    if field.minimum != None or field.maximum != None:
        ret_val.append(range_rule(field, field_name))
    return ret_val





def decimal_rule(field, field_name):
    if field.type == "float":
        return []
    ret_val = []
    if field.scale != None:
        quality_rule = {"name": "check_" + field_name + "_scale",
                    "kind": "CONDITION",
                    "mode": "WRITE"                         
                }
        if field.required == False:   
            quality_rule["type"] = "CEL_FIELD"
            quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'DOUBLE' ; string(value).matches(r\"^\\d+(\\.\\d{0," + str(field.scale) + "})?$\")"
        else:
            quality_rule["type"] = "CEL"
            quality_rule["expr"] = "string(message." + field_name + ").matches(r\"^\\d+(\\.\\d{0," + str(field.scale) + "})?$\")"
        quality_rule = set_dlq_config(quality_rule, field)
        ret_val.append(quality_rule)
    if field.precision != None:
        quality_rule = {"name": "check_" + field_name + "_precision",
                    "kind": "CONDITION",
                    "mode": "WRITE"
                }
        if field.required == False:
            quality_rule["type"] = "CEL_FIELD"
            quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'DOUBLE' ; string(value).matches(\"^-?\\d{1," + str(field.precision) + "}\\.?\\d*$\")"
        else:
            quality_rule["type"] = "CEL"
            quality_rule["expr"] = "string(message." + field_name + ").matches(\"^-?\\d{1," + str(field.precision) + "}\\.?\\d*$\")"
        if field.config and "dq_dlq_topic" in field.config:
            quality_rule["onFailure"] = "DLQ"
            quality_rule["params"] = { "dlq.topic": field.config["dq_dlq_topic"] }
        else:
            quality_rule["onFailure"] = "ERROR"
        ret_val.append(quality_rule)
    if field.minimum != None or field.maximum != None:
       quality_rule = range_rule(field, field_name)
       ret_val.append(quality_rule)
    return ret_val



def range_rule(field, field_name):
    if field.minimum != None and field.maximum != None:
        quality_rule = {"name": "check_" + field_name + "_range",
                    "kind": "CONDITION",
                    "mode": "WRITE"
                }
        if field.required == False:
            if field.type in ["integer", "int", "date"]:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'INT' ; value >= " + str(field.minimum) + " && value <= " + str(field.maximum)
            elif field.type in ["long", "bigint", "timestamp", "timestamp_tz", "timestamp_ntz", "time"]:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'LONG' ; value >= " + str(field.minimum) + " && value <= " + str(field.maximum)
            elif field.type == "float":
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'FLOAT' ; value >= " + str(field.minimum) + " && value <= " + str(field.maximum)
            else:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'DOUBLE' ; value >= " + str(field.minimum) + " && value <= " + str(field.maximum)
        else:
            quality_rule["type"] = "CEL"
            quality_rule["expr"] = "message." + field_name + " >= " + str(field.minimum) + " && message." + field_name + " <= " + str(field.maximum)
    elif field.minimum != None:
        quality_rule = {"name": "check_" + field_name + "_range",
                    "kind": "CONDITION",
                    "mode": "WRITE"
                }
        if field.required == False:
            if field.type in ["integer", "int", "date"]:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'INT' ; value >= " + str(field.minimum) 
            elif field.type in ["long", "bigint", "timestamp", "timestamp_tz", "timestamp_ntz", "time"]:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'LONG' ; value >= " + str(field.minimum)
            elif field.type == "float":
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'FLOAT' ; value >= " + str(field.minimum)
            else:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'DOUBLE' ; value >= " + str(field.minimum) 
        else:
            quality_rule["type"] = "CEL"
            quality_rule["expr"] = "message." + field_name + " >= " + str(field.minimum)
    elif field.maximum != None:
        quality_rule = {"name": "check_" + field_name + "_range",
                    "kind": "CONDITION",
                    "mode": "WRITE"
                }
        if field.required == False:
            if field.type in ["integer", "int", "date"]:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'INT' ; value <= " + str(field.maximum)
            elif field.type in ["long", "bigint", "timestamp", "timestamp_tz", "timestamp_ntz", "time"]:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'LONG' ; value <= " + str(field.maximum)
            elif field.type == "float":
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'FLOAT' ; value <= " + str(field.maximum)
            else:
                quality_rule["type"] = "CEL_FIELD"
                quality_rule["expr"] = "name == '" + field_name + "' && typeName == 'DOUBLE' ; value <= " + str(field.maximum)
        else:
            quality_rule["type"] = "CEL"
            quality_rule["expr"] = "message." + field_name + " <= " + str(field.maximum)
    quality_rule = set_dlq_config(quality_rule, field)
    return quality_rule


def set_dlq_config(quality_rule, field):
    if field.config and "dq_dlq_topic" in field.config:
        quality_rule["onFailure"] = "DLQ"
        quality_rule["params"] = { "dlq.topic": field.config["dq_dlq_topic"] }
    else:
        quality_rule["onFailure"] = "ERROR"
    return quality_rule