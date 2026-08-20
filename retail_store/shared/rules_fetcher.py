# from databricks import sql
# import os


def get_validation_rules():
    """Returns a centralized dictionary of data quality constraints."""
    return [
        {
            "pipeline_name" : "employees-etl",
            "dataset_name" : "silver_employees",
            "name": "valid_id",
            "constraint": "employee_id IS NOT NULL",
            "tag": ["validity", "employee"]
        },
        {
            "pipeline_name" : "employees-etl",
            "dataset_name" : "silver_employees",
            "name": "valid_email",
            "constraint": "email LIKE '%@%.%'",
            "tag": ["validity", "employee"]
        },
        {
            "pipeline_name" : "location-etl",
            "dataset_name" : "silver_location",
            "name": "valid_id",
            "constraint": "location_id IS NOT NULL",
            "tag": ["validity", "location"]
        },
         {
            "pipeline_name" : "departments-etl",
            "dataset_name" : "silver_departments",
            "name": "valid_id",
            "constraint": "dept_id IS NOT NULL",
            "tag": ["validity", "departments"]
        }
    ]



def get_rules(pipeline_name, dataset_name):
  """
    loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  return {
    row['name']: row['constraint']
    for row in get_validation_rules()
    if row['pipeline_name'] == pipeline_name and row['dataset_name'] == dataset_name
  }


# rules = get_validation_rules()

# def fetch_db_rules(pipeline_name: str, dataset_name: str) -> dict:
#     """Queries the central Delta table to return an SDP-compatible expectations dict."""

#     qry = f"""
#         SELECT *
#         FROM retail_store.governance.pipeline_validation_rules
#         WHERE pipeline_name = '{pipeline_name}'
#         AND dataset_name = '{dataset_name}'
#         """

#     with sql.connect(server_hostname = os.getenv(
#     """