import json
from pyspark.sql.types import *
from pyspark.sql import types

class Schema:
    def __init__(self, json_schema):
        self.schema = StructType()
        self.json_schema = json_schema
    
    def create_schema(self):
        for field in self.json_schema:
            sch_type = getattr(types, field.get('value_type'))
            cls = sch_type.__call__()
            self.schema.add(StructField(field.get('column_name'), cls, field.get('required')))

    def get_schema(self):
        self.create_schema()
        return self.schema



    