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
        :param files: Dictionary Object config['input']
        :return:  Returns a : Dataframe
        """
        # Input Files paths
        source_path = files['inputpath']
        units_use_csv_path = source_path + "/" + files["units"]
        charges_use_csv_path = source_path + "/" + files["charges"]
        damages_use_csv_path = source_path + "/" + files["damages"]

        # Reads the CSV files
        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)
        charges_df = Utils.load_csv(session=session, path=charges_use_csv_path, header=True,
                                  schema=schemas.charges_schema)
        damages_df = Utils.load_csv(session=session, path=damages_use_csv_path, header=True,
                                  schema=schemas.damages_schema)
        # filtered_data = units_df \
        #     .where(((col("VEH_DMAG_SCL_1_ID") == "NO DAMAGE") &
        #             (col("VEH_DMAG_SCL_2_ID") == 'NO DAMAGE')) |
        #            ((regexp_extract("VEH_DMAG_SCL_1_ID", '\d+', 0) > 4)
        #             & (regexp_extract("VEH_DMAG_SCL_2_ID", '\d+', 0) > 4))
        #            & col("FIN_RESP_TYPE_ID").contains("INSURANCE")
        #            ).select("CRASH_ID",
        #                     "FIN_RESP_TYPE_ID",
        #                     "VEH_DMAG_SCL_1_ID",
        #                     "VEH_DMAG_SCL_2_ID")

        # unique_crash_id = filtered_data.select("CRASH_ID").distinct().count()

        no_insurance_df = charges_df.filter(col('CHARGE').contains('NO INSURANCE')).select('CRASH_ID','UNIT_NBR').withColumnRenamed('CRASH_ID','I_CRASH_ID').withColumnRenamed('UNIT_NBR','I_UNIT_NBR')
        damaged_property_df = damages_df.filter(col('DAMAGED_PROPERTY').contains('NO DAMAGE')).select('CRASH_ID').distinct().withColumnRenamed('CRASH_ID','D_CRASH_ID')
        combined_df= units_df.join(no_insurance_df,(units_df['CRASH_ID']==no_insurance_df['I_CRASH_ID']) & (units_df['UNIT_NBR']==no_insurance_df['I_UNIT_NBR']),how='left')
        units_combined_df = combined_df.filter("I_CRASH_ID is null").select('CRASH_ID','UNIT_NBR','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID')
        damaged_combined_df = units_combined_df.join(damaged_property_df,units_combined_df['CRASH_ID']==damaged_property_df['D_CRASH_ID'],how='left')
        all_combined_df= damaged_combined_df.filter("D_CRASH_ID is not null").select('CRASH_ID','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID') 
        result_df = all_combined_df.withColumn('DMAG1_RANGE',regexp_extract(col('VEH_DMAG_SCL_1_ID'), "\\d+", 0)) \
                        .withColumn('DMAG2_RANGE',regexp_extract(col('VEH_DMAG_SCL_2_ID'), "\\d+", 0)) \
                        .filter("DMAG1_RANGE > 4 or DMAG2_RANGE > 4") \
                        .select('CRASH_ID').distinct().count()
        return session.createDataFrame([("Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance ",result_df)],'a string, b string')


    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: config['input']
        :return: Dataframe
        """
        return SafeCrashes.__process(SafeCrashes, session, files)