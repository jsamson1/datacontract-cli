import json
import os
import sys

from typer.testing import CliRunner

from datacontract.cli import app
from datacontract.data_contract import DataContract
from datacontract.export.confluent_data_quality_converter import to_ruleset_json
from datacontract.model.data_contract_specification import DataContractSpecification

# logging.basicConfig(level=logging.DEBUG, force=True)


def test_cli():
    runner = CliRunner()
    result = runner.invoke(app, ["export", "./fixtures/confluent-data-quality/datacontract_dqRules.yaml", "--format", "confluent-data-quality"])
    assert result.exit_code == 0


def test_to_rulesetjson():
    #REMOVE "tests/"
    data_contract = DataContractSpecification.from_file("fixtures/confluent-data-quality/datacontract_dqRules.yaml")

    with open("fixtures/confluent-data-quality/datacontract_dqRules.json") as file:
        expected_json_ruleset = file.read()

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    assert json.loads(result) == json.loads(expected_json_ruleset)

def test_to_rulesetjson_with_optional():
    #REMOVE "tests/"
    data_contract = DataContractSpecification.from_file("fixtures/confluent-data-quality/datacontract_dqRules_optional.yaml")

    with open("fixtures/confluent-data-quality/datacontract_dqRules_optional.json") as file:
        expected_json_ruleset = file.read()

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    assert json.loads(result) == json.loads(expected_json_ruleset)


def read_file(data_contract_file):
    if not os.path.exists(data_contract_file):
        print(f"The file '{data_contract_file}' does not exist.")
        sys.exit(1)
    with open(data_contract_file, "r") as file:
        file_content = file.read()
    return file_content
