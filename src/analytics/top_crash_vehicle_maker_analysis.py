from pyspark.sql.functions import col,count
from utils.utils import Utils
from utils import schemas


class TopCrashVehicleMaker:
    """
    Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
    """

    def __process(self, session, files):
        """
        Finds out Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
        :param session: SparkSession
        :param files: Yaml config['files']
        :return:  Returns a : Int
        """

        # Loads input files path into variables
        source_path = files['inputpath']
        person_use_csv_path = source_path + "/" + files["person"]
        units_use_csv_path = source_path + "/" + files["units"]

        # Loads the inputs files data
        primary_person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)
        
        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)
        
        primary_person_df=primary_person_df.filter((primary_person_df.PRSN_TYPE_ID == 'DRIVER') & (primary_person_df.DEATH_CNT >0) & (primary_person_df.PRSN_AIRBAG_ID == 'NOT DEPLOYED'))

        return primary_person_df.join(units_df,["CRASH_ID", "UNIT_NBR"],'inner').groupBy(units_df.VEH_MAKE_ID).agg(count("*").alias("total_crashes")).orderBy(col("total_crashes").desc()).limit(5)

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Integer -> Total No of crashes
        """
        return TopCrashVehicleMaker.__process(TopCrashVehicleMaker, session, files)