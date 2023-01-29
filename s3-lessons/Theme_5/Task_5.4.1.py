from airflow.hooks.base import BaseHook
import json

conn_s3 = BaseHook.get_connection(conn_s3)

conn_s3_extra = json.loads('YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA')

print(conn_s3_extra)
