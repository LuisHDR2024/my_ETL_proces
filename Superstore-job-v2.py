#---------------------------------------------------------
# configuraciones para el funccionamiento del job notebook
#---------------------------------------------------------


#---------------------------------------------------------
# importaciones estandares para AWS Glue
#---------------------------------------------------------

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

#---------------------------------------------------------
# funciones para el inicio de la sesion interactiva de glue
#---------------------------------------------------------

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


from pyspark.sql.functions import col, regexp_replace
from pyspark.sql import functions as F

#---------------------------------------------
# Procesar dataframe con líneas corruptas
#---------------------------------------------

import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, regexp_replace

args = getResolvedOptions(sys.argv, ['input_file','output_folder'])
input_file = args['input_file']
output_folder = args['output_folder']

#----------------------------------------------------
# iniciar procesamiento de datos
#----------------------------------------------------

df_text = spark.read.text(input_file)
df_bad = df_text.filter(~col("value").rlike(r'^([^,]*,)*[^,]*$'))
df_fixed = df_text.withColumn(
    "value",
    regexp_replace("value", '"(?=[^,]*$)', "'")
)
rdd = df_fixed.rdd.map(lambda row: row[0])
#spark_df_csv = spark.read.csv(rdd, header=True, inferSchema=True)

spark_df_csv = spark.read.csv(
    rdd,
    header=True,
    inferSchema=True,
    escape='"',  # Importante para manejar comillas escapadas
    multiLine=True  # Importante si hay saltos de línea dentro de campos
)

#---------------------------------------------
# Inicar transformacion de data
#---------------------------------------------

# 0. Valores establecidos (basado en EDA local)
Sales_median = 54.38
Sales_lower_bound  = 2
Sales_upper_bound  = 1000

Quantity_median = 3
Quantity_lower_bound = 1
Quantity_upper_bound = 10

Discount_median = 0.2
Discount_lower_bound = 0.0
Discount_upper_bound = 0.9

# 1. Castear a numericos correspondientes
spark_df_csv = spark_df_csv.withColumn("Sales", F.col("Sales").cast("float"))
spark_df_csv = spark_df_csv.withColumn("Quantity", F.col("Quantity").cast("int"))
spark_df_csv = spark_df_csv.withColumn("Discount", F.col("Discount").cast("float"))

# 2. remplazar nulos con mediana
spark_df_csv = spark_df_csv.na.fill({"Sales": Sales_median})
spark_df_csv = spark_df_csv.na.fill({"Quantity": Quantity_median})
spark_df_csv = spark_df_csv.na.fill({"Discount": Discount_median})

# 3. remplazar outliers con mediana
def replace_outliers(df, col, lower, upper, median):
    return df.withColumn(
        col,
        F.when((F.col(col) < lower) | (F.col(col) > upper), median)
         .otherwise(F.col(col))
    )

spark_df_csv = replace_outliers(spark_df_csv, "Sales", Sales_lower_bound, Sales_upper_bound, Sales_median)
spark_df_csv = replace_outliers(spark_df_csv, "Quantity", Quantity_lower_bound, Quantity_upper_bound, Quantity_median)
spark_df_csv = replace_outliers(spark_df_csv, "Discount", Discount_lower_bound, Discount_upper_bound, Discount_median)

#---------------------------------------------
# Guardar CSV procesado
#---------------------------------------------

spark_df_csv.coalesce(1).write \
    .mode("append") \
    .option("header", "true") \
    .csv(output_folder)

print(f"CSV guardado en: {output_folder}")

job.commit()