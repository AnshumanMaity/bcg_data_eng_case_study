from .logger import logger
import json


class Utils:

    @staticmethod
    def load_csv(session, path, header, schema, delimiter=","):
        """
        This methods reads the CSV files
        :param session: SparkSession -> ~`pyspark.sql.SparkSession`
        :param path:  String -> CSV file path
        :param header: Boolean -> Header columns
        :param schema: Schema  -> pyspark.sql.types.StructType
        :param delimiter: string -> Default ","
        :return: DataFrame ->     `pyspark.sql.DataFrame`

        """
        try:
            data_df = session.read \
                .format("csv") \
                .option("delimiter", delimiter) \
                .schema(schema) \
                .option("header", header) \
                .option("path", path) \
                .load()
            logger.debug("Read the file- %s", path)
            return data_df
        except Exception as err:
            logger.error("%s, Error: %s", str(__name__), str(err))

    @staticmethod
    def load_config(config_file):
        """Loads the configuration file and returns it as a dictionary."""
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config