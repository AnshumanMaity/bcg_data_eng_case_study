from pyspark.sql.functions import col,upper
from utils.utils import Utils
from utils import schemas


class TwoWheelersCrashAnalysis:
    """
    Analysis 2: How many two wheelers are booked for crashes?
    """

    def __process(self, session, files):
        """
        Analyzes how many two wheelers are booked for crashes.
        :param session: SparkSession
        :param files: Dictionary Object config['input']
        :return:  Returns a : Dataframe
        """

        # Loads input files path into variables
        source_path = files['inputpath']
        units_use_csv_path = source_path + "/" + files["units"]

        # Loads the primary person data into df
        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)
        result=units_df.filter(upper(col('VEH_BODY_STYL_ID')).like('%MOTORCYCLE%')).count()

        #Calculates and returns how many two wheelers are booked for crashes.
        return session.createDataFrame([("Analysis 2: How many two wheelers are booked for crashes?",result)],'a string, b long')

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get the analysis report
        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Dataframe -> Total No of crashes
        """
        return TwoWheelersCrashAnalysis.__process(TwoWheelersCrashAnalysis, session, files)