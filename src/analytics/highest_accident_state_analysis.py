from pyspark.sql.functions import col,count
from utils.utils import Utils
from utils import schemas


class HighestAccidentsState:
    """
    Analysis 5: Which state has highest number of accidents in which females are not involved? 
    """

    def __process(self, session, files):
        """
        Finds out state has highest number of accidents in which females are not involved?
        :param session: SparkSession
        :param files: Dictionary Object config['input']
        :return:  Returns a : Dataframe
        """

        source_path = files['inputpath']
        person_use_csv_path = source_path + "/" + files["person"]

        # Loads the files data
        person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)

        top_state_crashes = person_df.filter(person_df.PRSN_GNDR_ID != "FEMALE") \
            .groupBy("DRVR_LIC_STATE_ID") \
            .agg(count(col("CRASH_ID")).alias("TotalCrashes")) \
            .orderBy(col("TotalCrashes").desc())\
            .first()
        result = str(str(top_state_crashes["DRVR_LIC_STATE_ID"] )+ " - " + str(top_state_crashes["TotalCrashes"]))
        return session.createDataFrame([("Analysis 5: Which state has highest number of accidents in which females are not involved? ",result)],'a string, b string')

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: config['input']
        :return: Dataframe -> Total No of crashes
        """
        return HighestAccidentsState.__process(HighestAccidentsState, session, files)