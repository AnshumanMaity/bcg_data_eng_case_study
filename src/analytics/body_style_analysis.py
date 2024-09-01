from pyspark.sql.functions import col, sum, desc, dense_rank
from utils.utils import Utils
from utils import schemas
from pyspark.sql import Window


class BodyStyleAnalysis:
    """
    Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
    """

    def __process(self, session, files):
        """
        Finds out Top ethnic user group of each unique body style involved in crashes
        :param session: SparkSession
        :param files: Yaml config['files']
        :return:  Returns a : Int
        """
        source_path = files['inputpath']
        units_use_csv_path = source_path + "/" + files["units"]

        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)

        top_vehicles_crashes = units_df.groupBy("VEH_MAKE_ID") \
            .agg(sum("DEATH_CNT").alias("TOTAL_DEATHS")) \
            .orderBy(col("TOTAL_DEATHS"))

        window = Window.orderBy(desc("TOTAL_DEATHS"))

        top_range = top_vehicles_crashes. \
            withColumn("id", dense_rank().over(window)) \
            .filter((col("id") >= 3) & (col("id") <= 5)).drop("id")


        return top_range

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Integer -> Total No of crashes
        """
        return BodyStyleAnalysis.__process(BodyStyleAnalysis, session, files)