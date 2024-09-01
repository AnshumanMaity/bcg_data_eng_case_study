from pyspark.sql.functions import col,count
from utils.utils import Utils
from utils import schemas


class ValidLicenseHitRunAnalysis:
    """
    Analysis 4: Determine number of Vehicles with driver having valid licenses involved in hit and run? 
    """

    def __process(self, session, files):
        """
        Finds out number of Vehicles with driver having valid licenses involved in hit and run? .
        :param session: SparkSession
        :param files: Yaml config['files']
        :return:  Returns a : Int
        """

        # Input files path
        source_path = files['inputpath']

        person_use_csv_path = source_path + "/" + files["person"]

        # Loads the primary person data into df
        primary_person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)
        primary_person_df=primary_person_df.where("PRSN_TYPE_ID = 'DRIVER' AND DRVR_LIC_TYPE_ID in ('DRIVER LICENSE','COMMERCIAL DRIVER LIC.')")

        

        units_use_csv_path = source_path + "/" + files["units"]

        # Loads the inputs files data
        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)
        units_df=units_df.filter(units_df.VEH_HNR_FL == 'Y')

        return primary_person_df.join(units_df,["CRASH_ID", "UNIT_NBR"],'inner').count()

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Integer -> Total No of crashes
        """
        return ValidLicenseHitRunAnalysis.__process(ValidLicenseHitRunAnalysis, session, files)