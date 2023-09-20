import json
import re
import os

PATH_DBT_PROJECT = "/home/caio/projects/queries-rj-smtr"

search_str = 'o=[i("manifest","manifest.json"+t),i("catalog","catalog.json"+t)]'

with open(os.path.join(PATH_DBT_PROJECT, "target", "index.html"), "r") as f:
    content_index = f.read()

with open(os.path.join(PATH_DBT_PROJECT, "target", "manifest.json"), "r") as f:
    json_manifest = json.loads(f.read())

# In the static website there are 2 more projects inside the documentation: dbt and dbt_bigquery
# This is technical information that we don't want to provide to our final users, so we drop it
# Note: depends of the connector, here we use BigQuery
IGNORE_PROJECTS = ["dbt", "dbt_bigquery"]
for element_type in [
    "nodes",
    "sources",
    "macros",
    "parent_map",
    "child_map",
]:  # navigate into manifest
    # We transform to list to not change dict size during iteration, we use default value {} to handle KeyError
    for key in list(json_manifest.get(element_type, {}).keys()):
        for ignore_project in IGNORE_PROJECTS:
            if re.match(
                rf"^.*\.{ignore_project}\.", key
            ):  # match with string that start with '*.<ignore_project>.'
                del json_manifest[element_type][key]  # delete element

with open(os.path.join(PATH_DBT_PROJECT, "target", "catalog.json"), "r") as f:
    json_catalog = json.loads(f.read())

with open(os.path.join(PATH_DBT_PROJECT, "target", "index2.html"), "w") as f:
    new_str = (
        "o=[{label: 'manifest', data: "
        + json.dumps(json_manifest)
        + "},{label: 'catalog', data: "
        + json.dumps(json_catalog)
        + "}]"
    )
    new_content = content_index.replace(search_str, new_str)
    f.write(new_content)
