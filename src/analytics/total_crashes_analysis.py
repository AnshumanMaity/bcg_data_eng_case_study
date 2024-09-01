from pyspark.sql.functions import col
from utils.utils import Utils
from utils import schemas


class MaleAccidentAnalysis:
    """
    Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
    """

    def __process(self, session, files):
        """
        :param session: SparkSession
        :param files: Yaml config['files']
        :return:  Returns a : Int
        """
        # Loads input files path into variables
        source_path = files['inputpath']
        person_use_csv_path = source_path + "/" + files["person"]

        # Loads the primary person data into df
        primary_person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)

        #Calculates the no of accidents where males killed are greater than 2
        total_crashes_males = primary_person_df.where((col("PRSN_GNDR_ID") == 'MALE') & (col("DEATH_CNT") > 2)).count()

        return total_crashes_males

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: Spark Session object
        :param files: Config files
        :return: Integer -> Total No of crashes
        """
        return MaleAccidentAnalysis.__process(MaleAccidentAnalysis, session, files)
