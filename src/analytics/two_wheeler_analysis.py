from pyspark.sql.functions import col
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
        :param files: Yaml config['files']
        :return:  Returns a : Int
        """

        # Input files path
        source_path = files['inputpath']
        units_use_csv_path = source_path + "/" + files["units"]

        # Loads the inputs files data
        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)

        return units_df.filter(col("VEH_BODY_STYL_ID") == "MOTORCYCLE").count()

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Integer -> Total No of crashes
        """
        return TwoWheelersCrashAnalysis.__process(TwoWheelersCrashAnalysis, session, files)