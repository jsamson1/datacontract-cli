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
    result = runner.invoke(app, ["export", "./tests/fixtures/confluent-data-validation/datacontract_dqRules.yaml", "--format", "confluent-data-validation"])
    assert result.exit_code == 0


def test_to_rulesetjson():
    #REMOVE "tests/"
    data_contract = DataContractSpecification.from_file("tests/fixtures/confluent-data-validation/datacontract_dqRules.yaml")

    with open("tests/fixtures/confluent-data-validation/datacontract_dqRules.json") as file:
        expected_json_ruleset = file.read()

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    assert json.loads(result) == json.loads(expected_json_ruleset)

def test_to_rulesetjson_with_optional():
    #REMOVE "tests/"
    data_contract = DataContractSpecification.from_file("tests/fixtures/confluent-data-validation/datacontract_dqRules_optional.yaml")

    with open("tests/fixtures/confluent-data-validation/datacontract_dqRules_optional.json") as file:
        expected_json_ruleset = file.read()

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    assert json.loads(result) == json.loads(expected_json_ruleset)

def test_to_rulesetjson_new():
    #REMOVE "tests/"
    data_contract = DataContractSpecification.from_file("tests/fixtures/confluent-data-validation/datacontract_new_contract.yaml")

    with open("tests/fixtures/confluent-data-validation/datacontract_new_contract.json") as file:
        expected_json_ruleset = file.read()

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    print(result)
    print(json.loads(expected_json_ruleset))

    assert json.loads(result) == json.loads(expected_json_ruleset)

def test_bib_contract():
    #REMOVE "tests/"
    data_contract = DataContractSpecification.from_file("tests/fixtures/confluent-data-validation/datacontract_destiny-library-bib.yaml")

    with open("tests/fixtures/confluent-data-validation/datacontract_destiny-library-bib.json") as file:
        expected_json_ruleset = file.read()

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)


    assert json.loads(result) == json.loads(expected_json_ruleset)

def test_copy_contract():
    #REMOVE "tests/"
    data_contract = DataContractSpecification.from_file("tests/fixtures/confluent-data-validation/datacontract_destiny-library-copy.yaml")

    with open("tests/fixtures/confluent-data-validation/datacontract_destiny-library-copy.json") as file:
        expected_json_ruleset = file.read()

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    assert json.loads(result) == json.loads(expected_json_ruleset)

def test_copy_status_contract():
    #REMOVE "tests/"
    data_contract = DataContractSpecification.from_file("tests/fixtures/confluent-data-validation/datacontract_destiny-library-copy-status.yaml")

    with open("tests/fixtures/confluent-data-validation/datacontract_destiny-library-copy-status.json") as file:
        expected_json_ruleset = file.read()

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    assert json.loads(result) == json.loads(expected_json_ruleset)

def print_json():
    #REMOVE "tests/"
    data_contract = DataContractSpecification.from_file("tests/fixtures/confluent-data-validation/datacontract_destiny-library-bib.yaml")

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    print(result)

    data_contract = DataContractSpecification.from_file("tests/fixtures/confluent-data-validation/datacontract_destiny-library-copy.yaml")

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    print(result)

    data_contract = DataContractSpecification.from_file("tests/fixtures/confluent-data-validation/datacontract_destiny-library-copy-status.yaml")

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    print(result)
    
def print_sample_json():
    #REMOVE "tests/"
    data_contract = DataContractSpecification.from_file("tests/fixtures/confluent-data-validation/datacontract_sample.yaml")

    model = next(iter(data_contract.models.items()))[1]
    result = to_ruleset_json(model)

    print(result)

def read_file(data_contract_file):
    if not os.path.exists(data_contract_file):
        print(f"The file '{data_contract_file}' does not exist.")
        sys.exit(1)
    with open(data_contract_file, "r") as file:
        file_content = file.read()
    return file_content