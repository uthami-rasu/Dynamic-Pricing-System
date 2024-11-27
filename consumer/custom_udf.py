

from pyspark.sql.functions import udf 
from pyspark.sql.types import IntegerType
from utils.connections import SPARK_SESSION 
import requests

import os 

@udf(IntegerType())
def hasDocumentUdf(product_id: str):
    try:
        lambda_endpoint = "https://d5orpj61l5.execute-api.us-east-1.amazonaws.com/default/mongodb-record-checker"
        response = requests.get(lambda_endpoint, params={"product_id": product_id})
        if response.status_code == 200:
            result = response.json().get("isexists")
            return int(result > 0)
    except Exception as e:
        print(e)

    