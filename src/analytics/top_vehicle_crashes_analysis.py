from pyspark.sql.functions import col, sum, desc, dense_rank
from utils.utils import Utils
from utils import schemas
from pyspark.sql import Window


class TopVehicleCrashes:
    """
    Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death 
    """

    def __process(self, session, files):
        """
        Finds out Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death 
        :param session: SparkSession
        :param files: Dictionary Object config['input']
        :return:  Returns a : Dataframe
        """
        source_path = files['inputpath']
        units_use_csv_path = source_path + "/" + files["units"]

        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)

        # top_vehicles_crashes = units_df.groupBy("VEH_MAKE_ID") \
        #     .agg(sum("DEATH_CNT").alias("TOTAL_DEATHS")) \
        #     .orderBy(col("TOTAL_DEATHS"))
        top_vehicles_crashes = units_df.groupBy("VEH_MAKE_ID") \
        .agg(sum("DEATH_CNT").alias("TOTAL_DEATHS"), sum("TOT_INJRY_CNT").alias("TOTAL_INJURIES")) \
        .withColumn("TOTAL_DEATH_INJURIES_CNT", col("TOTAL_DEATHS") + col("TOTAL_INJURIES")) \
        .orderBy(col("TOTAL_DEATH_INJURIES_CNT"))

        window = Window.orderBy(desc("TOTAL_DEATH_INJURIES_CNT"))

        top_range = top_vehicles_crashes. \
            withColumn("id", dense_rank().over(window)) \
            .filter((col("id") >= 3) & (col("id") <= 5)).drop("id")


        return top_range

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: config['input']
        :return: Dataframe
        """
        return TopVehicleCrashes.__process(TopVehicleCrashes, session, files)