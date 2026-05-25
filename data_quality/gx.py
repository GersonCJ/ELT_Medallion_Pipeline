import great_expectations as gx
import pandas as pd
from constants import path_strings
from pathlib import Path


def run_validation():

    # Create the Data context
    context = gx.get_context(mode="file", project_root_dir="/app")

    # Define the Data Source's parameters:
    # This path is relative to the `base_directory` of the Data Context.
    source_folder = Path(path_strings.bronze_path)
    data_source_name = "my_filesystem_data_source"

    # Create the Data Source:
    data_source = context.data_sources.add_or_update_pandas_filesystem(
        name=data_source_name, base_directory=source_folder
    )
        
    # Data Asset's parameters
    asset_name = "co2_raw_data_parquet"
    try:
        file_csv_asset = data_source.add_parquet_asset(name=asset_name)
    except Exception as e:
        file_csv_asset = data_source.get_asset(name=asset_name)
        
    # Batch definition
    file_data_asset = context.data_sources.get(data_source_name).get_asset(asset_name)

    batch_definition_name = "owid_co2_raw_data.parquet"
    try:
        batch_definition = file_csv_asset.add_batch_definition(name=batch_definition_name)
    except Exception as e:
        batch_definition = file_csv_asset.get_batch_definition(name=batch_definition_name)

    # Configure expectation suite to be called over runtime data
    expectation_suite = gx.ExpectationSuite("co2_quality_test")
    try:
        expectation_suite = context.suites.add(expectation_suite)
    except Exception as e:
        expectation_suite = context.suites.get("co2_quality_test")

    # Load data for configuration
    df_to_validate = pd.read_parquet(source_folder / batch_definition_name)

    # Define table level expectations
    columns = list(df_to_validate.columns)

    # Expectation 1 - Checking if columns "country" and "iso_code" are of type string
    for column in ["country", "iso_code"]:
        expectation_1 = gx.expectations.ExpectColumnValuesToBeOfType(
            column=column,
            type_="string"
        )
        expectation_suite.add_expectation(expectation_1)

    # Expectation 2 - Checking if columns "country", "year", "iso_code" are not Null
    for column in ["country", "year", "iso_code"]:
        expectation_2 = gx.expectations.ExpectColumnValuesToNotBeNull(
            column=column
        )
        expectation_suite.add_expectation(expectation_2)

    # Expectation 3 - Checking if the percent value columns are between 0 and 1
    for column in columns:
        if column.split("_")[-1] == "prct":
            expectation_3 = gx.expectations.ExpectColumnValuesToBeInSet(
                column=column,
                value_set=[0, 1]
            )
            expectation_suite.add_expectation(expectation_3)

    # Expectation 4 - Check if population column is greater than 0
    expectation_4 = gx.expectations.ExpectColumnMinToBeBetween(
        column="population",
        min_value=0
    )
    expectation_suite.add_expectation(expectation_4)

    # Expectation 5 - Check if number of iso_codes is within a possible range
    expectation_5 = gx.expectations.ExpectColumnUniqueValueCountToBeBetween(
        column="iso_code",
        min_value=193,
        max_value=249
    )
    expectation_suite.add_expectation(expectation_5)

    # Bundle suite and batch into validation definition and checkpoint w/ bundled
    # Actions for easy execution
    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=expectation_suite,
        name="co2_validations"
    )

    try:
        validation_definition = context.validation_definitions.add(validation_definition)
    except Exception as e:
        validation_definition = context.validation_definitions.get("co2_validations")    

    action_list = [
        gx.checkpoint.UpdateDataDocsAction(
            name="update_all_data_docs",
        ),
    ]
    checkpoint = gx.Checkpoint(
        name="co2_checkpoint",
        validation_definitions=[validation_definition],
        actions=action_list,
        result_format={
            "result_format":"COMPLETE",
        }
    )

    try:
        checkpoint = context.checkpoints.add(checkpoint)
    except:
        checkpoint = context.checkpoints.get("co2_checkpoint")

    # Run checkpoint to validate if everything runs properly
    runid = gx.RunIdentifier(run_name="co2_data")
    results = checkpoint.run(run_id=runid)