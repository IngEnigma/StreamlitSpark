from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("crime_data") \
        .getOrCreate()

    print("read dataset.csv ... ")
    
    path_crimes = "crime.csv"
    
    df_crimes = spark.read.csv(path_crimes, header=True, inferSchema=True)

    df_crimes = df_crimes.withColumnRenamed("DR_NO", "dr_no") \
                         .withColumnRenamed("Date Rptd", "report_date") \
                         .withColumnRenamed("DATE OCC", "date_occ") \
                         .withColumnRenamed("TIME OCC", "time_occ") \
                         .withColumnRenamed("AREA", "area") \
                         .withColumnRenamed("AREA NAME", "area_name") \
                         .withColumnRenamed("Rpt Dist No", "rpt_dist_no") \
                         .withColumnRenamed("Part 1-2", "part_1_2") \
                         .withColumnRenamed("Crm Cd", "crm_cd") \
                         .withColumnRenamed("Crm Cd Desc", "crm_cd_desc") \
                         .withColumnRenamed("Mocodes", "mocodes") \
                         .withColumnRenamed("Vict Age", "victim_age") \
                         .withColumnRenamed("Vict Sex", "victim_sex") \
                         .withColumnRenamed("Vict Descent", "victim_descent") \
                         .withColumnRenamed("Premis Cd", "premis_cd") \
                         .withColumnRenamed("Premis Desc", "premis_desc") \
                         .withColumnRenamed("Weapon Used Cd", "weapon_used_cd") \
                         .withColumnRenamed("Weapon Desc", "weapon_desc") \
                         .withColumnRenamed("Status", "status") \
                         .withColumnRenamed("Status Desc", "status_desc")

    df_crimes.createOrReplaceTempView("crimes")

    query = 'DESCRIBE crimes'
    spark.sql(query).show(20)

    query = """SELECT dr_no, report_date, victim_age, victim_sex, crm_cd_desc 
               FROM crimes WHERE victim_sex = 'M' 
               ORDER BY report_date"""
    df_male_crimes = spark.sql(query)
    df_male_crimes.show(20)

    query = """SELECT dr_no, report_date, victim_age, victim_sex, crm_cd_desc 
               FROM crimes WHERE report_date BETWEEN '2019-01-01' AND '2020-12-31' 
               ORDER BY report_date"""
    df_crimes_2019_2020 = spark.sql(query)
    df_crimes_2019_2020.show(20)

    results = df_crimes_2019_2020.toJSON().collect()
    with open('results/crimes_2019_2020.json', 'w') as file:
        json.dump(results, file)

    query = """SELECT area, COUNT(area) as crime_count 
               FROM crimes 
               GROUP BY area ORDER BY crime_count DESC"""
    df_crimes_by_area = spark.sql(query)
    df_crimes_by_area.show()

    query = """SELECT dr_no, report_date, victim_age, victim_sex, crm_cd_desc 
               FROM crimes WHERE victim_age BETWEEN 18 AND 30 
               ORDER BY victim_age"""
    df_young_adults_crimes = spark.sql(query)
    df_young_adults_crimes.show(20)

    spark.stop()
