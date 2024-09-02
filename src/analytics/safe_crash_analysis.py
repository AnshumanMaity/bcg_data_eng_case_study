from pyspark.sql.functions import col,regexp_extract
from utils.utils import Utils
from utils import schemas


class SafeCrashes:
    """
    Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    """

    def __process(self, session, files):
        """
        Finds out number of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        :param session: SparkSession
        :param files: Yaml config['files']
        :return:  Returns a : Int
        """
        # Input Files paths
        source_path = files['inputpath']
        units_use_csv_path = source_path + "/" + files["units"]

        # Reads the CSV files
        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)
        filtered_data = units_df \
            .where(((col("VEH_DMAG_SCL_1_ID") == "NO DAMAGE") &
                    (col("VEH_DMAG_SCL_2_ID") == 'NO DAMAGE')) |
                   ((regexp_extract("VEH_DMAG_SCL_1_ID", '\d+', 0) > 4)
                    & (regexp_extract("VEH_DMAG_SCL_2_ID", '\d+', 0) > 4))
                   & col("FIN_RESP_TYPE_ID").contains("INSURANCE")
                   ).select("CRASH_ID",
                            "FIN_RESP_TYPE_ID",
                            "VEH_DMAG_SCL_1_ID",
                            "VEH_DMAG_SCL_2_ID")

        unique_crash_id = filtered_data.select("CRASH_ID").distinct().count()
        return session.createDataFrame([("Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance ",unique_crash_id)],'a string, b string')


    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Integer -> Total No of crashes
        """
        return SafeCrashes.__process(SafeCrashes, session, files)