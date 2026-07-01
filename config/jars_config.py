# config/jars_config.py
import os

# config/jars_config.py
JARS_BASE = "C:/Users/admin/Desktop/log_search/jars"

SPARK_JARS = ",".join([
    f"file:///{JARS_BASE}hadoop-aws-3.3.4.jar",
    f"file:///{JARS_BASE}/aws-java-sdk-bundle-1.12.262.jar",
])