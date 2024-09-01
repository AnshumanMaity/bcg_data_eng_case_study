
from pyspark.sql import SparkSession, DataFrame
import argparse
from utils.utils import Utils
from dependencies import files
from utils.logger import logger
from analytics.analysis_total_crashes import MaleAccidentAnalysis

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="BCG Car Crash Case Study")
    parser.add_argument('--analytics_code', help="analytics code", default="analytics_code_1", )
    parser.add_argument('--output_file_path', help="output file path", default="/")
    parser.add_argument('--output_format', help="output file format", default="parquet")

    args = parser.parse_args()
    analytics_code = args.analytics_code
    output_path = analytics_code if args.output_file_path == '/' else args.output_file_path
    output_file_format = args.output_format

    logger.info(f'{analytics_code} processing is started')

    all_analytics_code = {
        "analytics_code_1": MaleAccidentAnalysis
    }
    try:
        spark = SparkSession \
            .builder \
            .config("spark.app.name", "BCG Car Crash Case Study") \
            .getOrCreate()

        # Selects the pipeline and starts processing
        result = all_analytics_code[analytics_code].execute(session=spark, files=files)

        if isinstance(result, DataFrame):
            Utils.save(result, file_format=output_file_format, output_path=output_path)
        else:
            print(f"OUTPUT OF {str(analytics_code).upper()}: {result}")

    except Exception as err:
        logger.error("%s Error : %s", __name__, str(err))

    finally:
        spark.stop()
        logger.debug(f"Completed execution of {analytics_code}")
        logger.debug("Successfully stopped spark session ")
