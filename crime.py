from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("CrimeData") \
        .getOrCreate()

    os.makedirs("results", exist_ok=True)  

    print("read dataset.csv ... ")
    
    path_crimes = "crime.csv"
    
    df_crimes = spark.read.csv(path_crimes, header=True, inferSchema=True)

    df_crimes = df_crimes.withColumnRenamed("Vict Age", "victim_age") \
                         .withColumnRenamed("Vict Sex", "victim_sex") \
                         .withColumnRenamed("Vict Descent", "victim_descent") \
                         .withColumnRenamed("Date Rptd", "report_date") \
                         .withColumnRenamed("Crm Cd Desc", "crm_cd_desc")

    df_crimes.createOrReplaceTempView("crimes")

    query = """SELECT DR_NO, report_date, victim_age, victim_sex, crm_cd_desc
               FROM crimes WHERE victim_sex = 'M' 
               ORDER BY report_date"""
    df_male_crimes = spark.sql(query)
    df_male_crimes.write.json("results/male_crimes", mode="overwrite")

    query = '''SELECT DR_NO, report_date, victim_age, victim_sex, crm_cd_desc 
               FROM crimes WHERE report_date BETWEEN '2019-01-01' AND '2020-12-31' 
               ORDER BY report_date'''
    df_crimes_2019_2020 = spark.sql(query)
    df_crimes_2019_2020.write.json("results/crimes_2019_2020", mode="overwrite")

    query = '''SELECT AREA, COUNT(AREA) as crime_count 
               FROM crimes 
               GROUP BY AREA ORDER BY crime_count DESC'''
    df_crimes_by_area = spark.sql(query)
    df_crimes_by_area.write.json("results/crimes_by_area", mode="overwrite")

    query = '''SELECT DR_NO, report_date, victim_age, victim_sex, crm_cd_desc
               FROM crimes WHERE victim_age BETWEEN 18 AND 30 
               ORDER BY victim_age'''
    df_young_adults_crimes = spark.sql(query)
    df_young_adults_crimes.write.json("results/young_adults_crimes", mode="overwrite")

    spark.stop()
