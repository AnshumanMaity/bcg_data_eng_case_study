
from pyspark.sql import SparkSession
import argparse
from utils.utils import Utils
from utils.logger import logger
from analytics import MaleCrashesAnalysis,TwoWheelersCrashAnalysis,TopCrashVehicleMaker,ValidLicenseHitRunAnalysis,HighestAccidentsState,TopVehicleCrashes,BodyStyleAnalysis,TopZipCodes,SafeCrashes,TopVehicleMakerschargedSpeeding

if __name__ == "__main__":

    #Parses the config value from the input json file.
    parser = argparse.ArgumentParser(description="BCG Car Crash Case Study")
    parser.add_argument("--config", required=True, type=str, default="config.json")
    args = parser.parse_args()

    #Loads config value into variable for further use.
    args_value = Utils.load_config(args.config)
    analytics_code = args_value["analytics_code"]
    output_path = args_value["output_path"]
    input_path = args_value["input"]

    logger.info(f'{analytics_code} processing is started')

    #Defines specific classes for 
    all_analytics_code = {
        "analytics_code_1": MaleCrashesAnalysis,
        "analytics_code_2": TwoWheelersCrashAnalysis,
        "analytics_code_3": TopCrashVehicleMaker,
        "analytics_code_4": ValidLicenseHitRunAnalysis,
        "analytics_code_5": HighestAccidentsState,
        "analytics_code_6": TopVehicleCrashes,
        "analytics_code_7": BodyStyleAnalysis,
        "analytics_code_8": TopZipCodes,
        "analytics_code_9": SafeCrashes,
        "analytics_code_10": TopVehicleMakerschargedSpeeding
    }
    try:
        spark = SparkSession \
            .builder \
            .config("spark.app.name", "BCG Car Crash Case Study") \
            .getOrCreate()
        
        # Selects the analytics code and starts processing
        result = all_analytics_code[analytics_code].execute(session=spark, files=input_path)

        #Saves the result into specific output folders
        result.repartition(1).write.mode("overwrite").csv(f"{output_path}/{analytics_code}", header=True)

    except Exception as err:
        logger.error("%s Error : %s", __name__, str(err))

    finally:
        spark.stop()
        logger.debug(f"Completed execution of {analytics_code}")
        logger.debug("Successfully stopped spark session ")
