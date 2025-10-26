# Databricks notebook source
# MAGIC %md
# MAGIC Creamos un DataFrame para manipular los datos con Spark

# COMMAND ----------

df = spark.table("workspace.aprendizaje.datos_empleados")

#Conteo de registros cargados
df.count()

# COMMAND ----------

#validamos registros duplicados por id
from pyspark.sql import functions as F

# Agrupar y detectar duplicados
df.groupBy("Employee_ID") \
  .agg(F.count("*").alias("conteo")) \
  .filter(F.col("conteo") > 1) \
  .show()


# COMMAND ----------

#dESCRIBIMOS EL DF
df.describe()
#cONTEO DE REGISTROS
df.count()

#Schema del df
df.printSchema()

# COMMAND ----------

df.describe()

# COMMAND ----------

df.display()

# COMMAND ----------

#Vamos a ver las funciones de agregado
from pyspark.sql import functions as F
# importamos col, sum, max, etc 
from pyspark.sql.functions import col, sum, max, min, avg

df.select(
  max(col("Salary")).alias("maximo de los salarios"),
  min(col("Salary")).alias("minimo de los salarios"),
  avg(col("Salary")).alias("promedio de los salarios"),
  sum(col("Salary")).alias("suma de los salarios"),
  max(col("Age")).alias("la edad mayor"),
  min(col("Age")).alias("la edad menor")
).display()


# COMMAND ----------

#creacion de datframe por genero
df_generos = df.select("Gender").distinct()

from pyspark.sql.functions import monotonically_increasing_id

df_generos = df_generos.withColumn("Id_Genero", monotonically_increasing_id())

df_generos.withColumn("Id_Genero", col("Genero"))

df_generos = df_generos.select(col("Id_Genero").cast("int"), "Gender")

df_generos.select(
  sum("Id_Genero")
).show()

df_generos.display()

df_generos.describe()

# COMMAND ----------

#Creacion df departamentos
df_departamentos = df.select("Department").distinct()
df_departamentos = df_departamentos.withColumn("Id_Departamento", monotonically_increasing_id())

df_departamentos = df_departamentos.select("Id_Departamento", "Department")

df_departamentos.display()


# COMMAND ----------

df_job_title = df.select("Job_Title").distinct()
df_job_title = df_job_title.withColumn("Id_Job_Title", monotonically_increasing_id())

df_job_title = df_job_title.select("Id_Job_Title", "Job_Title")

df_job_title.display()

# COMMAND ----------

df_education_level = df.select("Education_Level").distinct()
df_education_level = df_education_level.withColumn("Id_Education_Level", monotonically_increasing_id())

df_education_level = df_education_level.select(col("Id_Education_Level").cast("int"), "Education_Level")

df_education_level.display()

# COMMAND ----------

#Guardamos los catalogos creados:
# Guardar el DataFrame como tabla permanente en el metastore
df_generos.write.mode("overwrite").saveAsTable("workspace.aprendizaje.df_generos")
df_departamentos.write.mode("overwrite").saveAsTable("workspace.aprendizaje.df_departamentos")
df_job_title.write.mode("overwrite").saveAsTable("workspace.aprendizaje.df_job_title")
df_education_level.write.mode("overwrite").saveAsTable("workspace.aprendizaje.df_education_level")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN workspace;
# MAGIC

# COMMAND ----------

# Guardar los catálogos como tablas permanentes en el esquema 'aprendizaje'
df_generos.write.mode("overwrite").saveAsTable("aprendizaje.df_generos")
df_departamentos.write.mode("overwrite").saveAsTable("aprendizaje.df_departamentos")
df_job_title.write.mode("overwrite").saveAsTable("aprendizaje.df_job_title")
df_education_level.write.mode("overwrite").saveAsTable("aprendizaje.df_education_level")

