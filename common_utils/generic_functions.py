import pyspark.sql.functions as F
from pyspark.sql.types import *

def rename_columns(df, columns):

    for old_name, new_name in columns.items():

        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df



def safe_cast(df, mapping):

    INTEGRAL_TYPE = [ "byte", "short", "int", "integer", "long" ]

    for column, datatype in mapping.items():

        if column in df.columns:

            if datatype.lower() in INTEGRAL_TYPE:
                expression = f"try_cast(try_cast(`{column}` AS DOUBLE) AS {datatype})"
            else:
                expression = f"try_cast(`{column}` AS {datatype})"

            df = df.withColumn(column, F.expr(expression))

    return df