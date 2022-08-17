"""For educational purposes... Tables"""
import json
import os

#from gbq import bigquery
from google.cloud import bigquery

client = bigquery.Client()

dataset_name = 'ymu_data_test'
try:
    dataset = [d for d in client.list_datasets()
               if d.name == dataset_name][0]  # TODO!
except IndexError:
    print("Please, create dataset with name {}".format(dataset_name))
    raise


def make_schema_field(field, **kwargs):
    """
    Create an instance of a SchemaField from a given dict
    :param field: dict of SchemaField params
    :param kwargs: dict of description/fields of a SchemaField
    :return: instantiated SchemaField instance
    """
    # make recursive call for RECORD fields
    if field['type'] == "RECORD":
        kwargs['fields'] = [make_schema_field(f) for f in field['fields']]
    return bigquery.SchemaField(field['name'], field['type'], field['mode'],
                                **kwargs)


def create_table(schema_description, table_name):
    """
    Create BigQuery table based on given schema
    Example of schema file:
    [
        {
            "name": "timestamp",
            "type": "TIMESTAMP",
            "mode": "REQUIRED"
        },
        ...
        {
            "mode": "REPEATED",
            "name": "state",
            "type": "RECORD",
            "fields": [
                {
                    "mode": "REQUIRED",
                    "name": "param_name",
                    "type": "STRING"
                },
                {
                    "mode": "REQUIRED",
                    "name": "param_value",
                    "type": "STRING"
                }
            ]
        }
    ]
    :param schema_description: dict of table fields
    :param table_name: str of table name
    :return:
    """
    for field in schema_description:
        assert "name" in field, "no `name` in {}".format(field)
        assert "type" in field, "no `type` in {}".format(field)
        assert "mode" in field, "no `mode` in {}".format(field)

    schema = [make_schema_field(field) for field in schema_description]
    table = dataset.table(table_name, schema=schema)
    if not table.exists():
        table.create()
    return table


def main(path):
    table_name = os.path.basename(path).split('.', 1)[0]
    with open(path) as fh:
        schema = json.load(fh)
    create_table(schema, table_name)


# TODO: what to do with the views? 
# For the moment being at 2017-11-08 they are not used anywhere
# https://github.com/GoogleCloudPlatform/google-cloud-python/issues/3388#issuecomment-313523854
LAST_1_MIN_VIEW = """
SELECT * FROM [{table}]
  WHERE {column} = TIMESTAMP(CURRENT_DATE()) AND 
    TIMESTAMP >= TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 MINUTE))
ORDER BY TIMESTAMP LIMIT 1000
"""

LAST_5_MIN_VIEW = """
SELECT * FROM [{table}]
  WHERE {column} = TIMESTAMP(CURRENT_DATE()) AND 
    TIMESTAMP >= TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 5 MINUTE))
ORDER BY TIMESTAMP LIMIT 1000
"""

LAST_1_HOUR_VIEW = """
SELECT * FROM [{table}]
  WHERE {column} = TIMESTAMP(CURRENT_DATE()) AND 
    TIMESTAMP >= TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 HOUR))
ORDER BY TIMESTAMP LIMIT 1000
"""

LAST_1_DAY_VIEW = """
SELECT * FROM [{table}]
  WHERE {column} = TIMESTAMP(CURRENT_DATE()) AND 
    TIMESTAMP >= TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 DAY))
ORDER BY TIMESTAMP LIMIT 1000
"""

if __name__ == "__main__":
    main('../../bigquery/telemetry.schema.json')
    main('../../bigquery/errors.schema.json')