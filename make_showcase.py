import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import split
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def main():

#===================
# sample
#    df4pre = spark.read.option("header",False).csv(input_way)
#    df4pre.repartition(1).write.mode("overwrite").csv(out_way)
#===================

    #====================================================================
    # объявляем переменные передваемые в качестве параметров из bash
    input_way_one = sys.argv[1]
    input_way_two = sys.argv[2]
    output_way = sys.argv[3]
    #====================================================================


    #====================================================================
    # инициируем сессию spark
    conf = SparkConf().setAppName('make_showcase').setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    #====================================================================


    #====================================================================
    # зачитали файл с основными данными, загруженными на локальную машину
    df = spark.read.option("header", True).option("encoding", "UTF-8").csv(input_way_one)

    # зачитал справочник с кодами вызовов и их расшифровкой
    df_dict = spark.read.option("header", True).option("encoding", "UTF-8").csv(input_way_two)
    #====================================================================


    #====================================================================
    # убрали полные дубли строк
    df2 = df.distinct()
    #====================================================================


    #====================================================================
    # сделали view чтобы дальшей можно было к ней обращаться обычным SQL кодом
    df2.createOrReplaceTempView("df2")
    #====================================================================


    #====================================================================
    # подсчитываем кол-во NULL значений в таблице
    spark.sql("""select count(*) as qnt
    , sum(if(INCIDENT_NUMBER is null, 1, 0)) as INCIDENT_NUMBER
    , sum(if(OFFENSE_CODE is null, 1, 0)) as OFFENSE_CODE
    , sum(if(OFFENSE_CODE_GROUP is null, 1, 0)) as OFFENSE_CODE_GROUP
    , sum(if(OFFENSE_DESCRIPTION is null, 1, 0)) as OFFENSE_DESCRIPTION
    , sum(if(DISTRICT is null, 1, 0)) as DISTRICT
    , sum(if(REPORTING_AREA is null, 1, 0)) as REPORTING_AREA
    , sum(if(SHOOTING is null, 1, 0)) as SHOOTING
    , sum(if(OCCURRED_ON_DATE is null, 1, 0)) as OCCURRED_ON_DATE
    , sum(if(YEAR is null, 1, 0)) as YEAR
    , sum(if(MONTH is null, 1, 0)) as MONTH
    , sum(if(DAY_OF_WEEK is null, 1, 0)) as DAY_OF_WEEK
    , sum(if(HOUR is null, 1, 0)) as HOUR
    , sum(if(UCR_PART is null, 1, 0)) as UCR_PART
    , sum(if(STREET is null, 1, 0)) as STREET
    , sum(if(Lat is null, 1, 0)) as Lat
    , sum(if(Long is null, 1, 0)) as Long
    , sum(if(Location is null, 1, 0)) as Location
    from df2""").show()
    #====================================================================


    #====================================================================
    # подсчитываем кол-во уникальных значений в каждой колонке
    spark.sql("""select count(*) as qnt
    ,COUNT(DISTINCT(INCIDENT_NUMBER)) as INCIDENT_NUMBER
    ,COUNT(DISTINCT(OFFENSE_CODE)) as OFFENSE_CODE
    ,COUNT(DISTINCT(OFFENSE_CODE_GROUP)) as OFFENSE_CODE_GROUP
    ,COUNT(DISTINCT(OFFENSE_DESCRIPTION)) as OFFENSE_DESCRIPTION
    ,COUNT(DISTINCT(DISTRICT)) as DISTRICT
    ,COUNT(DISTINCT(REPORTING_AREA)) as REPORTING_AREA
    ,COUNT(DISTINCT(SHOOTING)) as SHOOTING
    ,COUNT(DISTINCT(OCCURRED_ON_DATE)) as OCCURRED_ON_DATE
    ,COUNT(DISTINCT(YEAR)) as YEAR
    ,COUNT(DISTINCT(MONTH)) as MONTH
    ,COUNT(DISTINCT(DAY_OF_WEEK)) as DAY_OF_WEEK
    ,COUNT(DISTINCT(HOUR)) as HOUR
    ,COUNT(DISTINCT(UCR_PART)) as UCR_PART
    ,COUNT(DISTINCT(STREET)) as STREET
    ,COUNT(DISTINCT(Lat)) as Lat
    ,COUNT(DISTINCT(Long)) as Long
    ,COUNT(DISTINCT(Location)) as Location
    from df2""").show()
    #====================================================================


    #====================================================================
    # уберем из выборки строки у которых номер района не заполнен а также оставляем выборку только с уникальными номерами инцидентов
    df3 = df2.select("INCIDENT_NUMBER","DISTRICT","YEAR","MONTH","Lat","Long").where("DISTRICT IS NOT NULL").distinct()
    #====================================================================


    #====================================================================
    # подсчет параметра crimes_total
    df_all_cr = df3.select("DISTRICT").groupBy("DISTRICT").count().alias("df_all_cr")

    df_all_cr.createOrReplaceTempView("df_all_cr")

    df_all_cr.show()
    #====================================================================


    #====================================================================
    # группируем данные по месяцам и сортируем чтобы дальше можно было посчитать медиану преступлений по районам
    cmp = df3.select("DISTRICT","YEAR","MONTH").where("DISTRICT IS NOT NULL").groupBy("DISTRICT","YEAR","MONTH").count().orderBy("DISTRICT","count")
    cmp.createOrReplaceTempView("cmp")

    # из того, что функция percentile_approx выдавала некорректный результат а именно:
    # при четном кол-ве строк брала не среднее значение между двух срединных значений а первое из двух срединных
    # пришлось написать логику определения медины:
    # для этого считаем ROW_NUMBER строк в прямом и обратном порядке с помощью двух СTE
    # и после считаем разницу этих порядков
    # значения разнициы 1 и -1 в группах с счетным кол-вом строк и 0 с группой нечетных строк
    # отбираем строки с указанной выше разницей и суммуруем в рамках DISTIRCT деля на кол-во строк в отобранной группе
    # и получаем медиану
    cmp2 = spark.sql("""
    WITH pre AS (
                    SELECT
                        *
                        ,ROW_NUMBER() OVER(PARTITION BY DISTRICT ORDER BY DISTRICT, count) AS rn_f
                    FROM cmp
                ),
    pre2 AS (
                SELECT
                    *
                    ,ROW_NUMBER() OVER(PARTITION BY DISTRICT ORDER BY DISTRICT, rn_f DESC) AS rn_b
                FROM pre        
            )

    SELECT
        *
        ,(rn_f-rn_b) AS srch_median
    FROM pre2
    WHERE (rn_f-rn_b) IN (1,-1,0)
    """)
    cmp2.createOrReplaceTempView("cmp2")

    crimes_monthly = spark.sql("""
    SELECT
        DISTRICT
        ,(SUM(count) / COUNT(DISTRICT)) AS crimes_median
    FROM cmp2
    GROUP BY 1
    """).alias("crimes_monthly")

    crimes_monthly.createOrReplaceTempView("crimes_monthly")

    crimes_monthly.select("*").show(20)
    #====================================================================


    #====================================================================
    # убираем дубли из словаря
    df_dict.createOrReplaceTempView("df_dict")
    df_dict_clear = spark.sql("""
        WITH pre AS (
                    SELECT
                        *
                        ,ROW_NUMBER()OVER(PARTiTION BY CODE ORDER BY CODE) AS rn            
                    FROM df_dict
                )
        SELECT
            *
        FROM pre
        WHERE rn = 1
    """).alias("df_dict_clear")

    # отделяем в справочнике первое слово из NAME
    fct_pre = df_dict_clear.withColumn("tmp_c",split(col("NAME"), " -",0)).select(col("CODE"),col("tmp_c").getItem(0).alias("crime_type"))
    fct_pre.createOrReplaceTempView("fct_pre")
    fct_pre.show(100,truncate=False)

    # отбираем целевые колонки для расчета frequent_crime_type
    temp_1 = df2.select("DISTRICT","OFFENSE_CODE").where("DISTRICT IS NOT NULL")
    temp_2 = temp_1.withColumn("OF_C",col("OFFENSE_CODE").cast("int")).drop("OFFENSE_CODE")
    temp_3 = temp_2.join(fct_pre, temp_2.OF_C == fct_pre.CODE,how='left').select("DISTRICT","CODE","crime_type")


    # подсчитываем оконной функцией частоту кодов и сортируем по убыванию чтобы потом найти ТОП-3 в разрезе района
    fct1 = temp_3.select("*").where("DISTRICT IS NOT NULL").groupBy("DISTRICT","crime_type").count().orderBy(("DISTRICT"),desc(("count")))
    fct1.show(100,truncate=False)

    # оконная функция чтобы пронумеровать и отобрать ТОП-3
    windowSpec = Window.partitionBy("DISTRICT").orderBy(desc("count"))
    fct2 = fct1.withColumn("rn",row_number().over(windowSpec)).where("rn < 4")
    fct2.createOrReplaceTempView("fct2")
    fct2.show(100,truncate=False)

    # блок в котором отобраннные ТОП-3 объединяем в одну ячейку в разрезе по районам
    fct3 = spark.sql("""
    WITH pre AS (
                    SELECT
                        DISTRICT
                        ,crime_type AS first_crime_type
                        ,LEAD(crime_type,1,0) OVER(PARTITION BY DISTRICT ORDER BY rn) AS second_crime_type
                        ,LEAD(crime_type,2,0) OVER(PARTITION BY DISTRICT ORDER BY rn) AS third_crime_type
                        ,rn
                    FROM fct2
                )

    SELECT
        DISTRICT
        ,CONCAT(first_crime_type,", ",second_crime_type,", ",third_crime_type) AS frequent_crime_type
    FROM pre
    WHERE rn = 1
    """).alias("fct3")

    fct3.createOrReplaceTempView("fct3")

    fct3.show(100,truncate=False)
    #====================================================================


    #====================================================================
    # расчет метрик по long & lat
    df3.createOrReplaceTempView("df3")

    lolat = spark.sql("""
    SELECT
        DISTRICT
        ,AVG(Lat) AS avg_lat
        ,AVG(Long) AS avg_long
    FROM df3
    GROUP BY 1
    """).alias("lolat")

    lolat.createOrReplaceTempView("lolat")

    lolat.select("*").show(100)
    #====================================================================


    #====================================================================
    # собираем финальную витрину
    temp_3 = temp_2.join(fct_pre, temp_2.OF_C == fct_pre.CODE,how='left').select("DISTRICT","CODE","crime_type")

    fin_tab = spark.sql("""
        SELECT
            df_all_cr.DISTRICT
            ,df_all_cr.count AS crimes_total
            ,crimes_monthly.crimes_median
            ,fct3.frequent_crime_type
            ,lolat.avg_lat
            ,lolat.avg_long
        FROM df_all_cr
        LEFT JOIN crimes_monthly
            ON df_all_cr.DISTRICT = crimes_monthly.DISTRICT
        LEFT JOIN fct3
            ON df_all_cr.DISTRICT = fct3.DISTRICT
        LEFT JOIN lolat
            ON df_all_cr.DISTRICT = lolat.DISTRICT
    """).alias("fin_tab")
    #====================================================================


    #====================================================================
    # записываем финальную таблицу на диск ПК
    fin_tab.repartition(1).write.mode("overwrite").csv(output_way)
    #====================================================================

if __name__ == "__main__":
    main()


