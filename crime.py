from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("CrimeData") \
        .getOrCreate()

    print("read dataset.csv ... ")
    
    path_crimes = "crime.csv"
    
    df_crimes = spark.read.csv(path_crimes, header=True, inferSchema=True)

    df_crimes = df_crimes.withColumnRenamed("Vict Age", "victim_age") \
                         .withColumnRenamed("Vict Sex", "victim_sex") \
                         .withColumnRenamed("Vict Descent", "victim_descent") \
                         .withColumnRenamed("Date Rptd", "report_date")

    df_crimes.createOrReplaceTempView("crimes")

    query = 'DESCRIBE crimes'
    spark.sql(query).show(20)

    query = """SELECT DR_NO, report_date, victim_age, victim_sex, Crm Cd Desc 
               FROM crimes WHERE victim_sex = 'M' 
               ORDER BY report_date"""
    df_male_crimes = spark.sql(query)
    df_male_crimes.show(20)

    query = '''SELECT DR_NO, report_date, victim_age, victim_sex, Crm Cd Desc 
               FROM crimes WHERE report_date BETWEEN '2019-01-01' AND '2020-12-31' 
               ORDER BY report_date'''
    df_crimes_2019_2020 = spark.sql(query)
    df_crimes_2019_2020.show(20)

    results = df_crimes_2019_2020.toJSON().collect()
    with open('results/crimes_2019_2020.json', 'w') as file:
        json.dump(results, file)

    query = '''SELECT AREA, COUNT(AREA) as crime_count 
               FROM crimes 
               GROUP BY AREA ORDER BY crime_count DESC'''
    df_crimes_by_area = spark.sql(query)
    df_crimes_by_area.show()

    query = '''SELECT DR_NO, report_date, victim_age, victim_sex, Crm Cd Desc 
               FROM crimes WHERE victim_age BETWEEN 18 AND 30 
               ORDER BY victim_age'''
    df_young_adults_crimes = spark.sql(query)
    df_young_adults_crimes.show(20)

    spark.stop()
