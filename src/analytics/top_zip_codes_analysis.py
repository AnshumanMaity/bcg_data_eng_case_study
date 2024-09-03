from pyspark.sql.functions import col
from utils.utils import Utils
from utils import schemas


class TopZipCodes:
    """
    Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    """

    def __process(self, session, files):
        """
        Finds out the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor
        :param session: SparkSession
        :param files: Dictionary Object config['input']
        :return:  Returns a : Dataframe
        """
        source_path = files['inputpath']
        person_use_csv_path = source_path + "/" + files["person"]
        units_use_csv_path = source_path + "/" + files["units"]

        # Loads the file data
        person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)
        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)

        # Dropping all the null values in Driver zip columns
        valid_person_df = person_df.na.drop(subset=["DRVR_ZIP"])

        top_zipcode_crashes = units_df.join(valid_person_df, ['CRASH_ID','UNIT_NBR'], how='inner') \
            .where(
            "VEH_BODY_STYL_ID in ('PASSENGER CAR, 4-DOOR', 'PASSENGER CAR, 2-DOOR') and  "
            "PRSN_ALC_RSLT_ID = 'Positive' "
        ).groupBy("DRVR_ZIP")\
            .count()\
            .orderBy(col("count").desc()).take(5)

        return session.createDataFrame(top_zipcode_crashes).limit(5)

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: config['input']
        :return: Dataframe
        """
        return TopZipCodes.__process(TopZipCodes, session, files)