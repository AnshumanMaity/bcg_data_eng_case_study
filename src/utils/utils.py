from .logger import logger


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
    def save(dataframe, file_format, output_path):
        """

        :param dataframe:  DataFrame -> `pyspark.sql.DataFrame`
        :param file_format:  String -> Output file format, eg: Parquet,ORC,CSV
        :param output_path: String -> Output path
        :return: None
        """
        try:
            dataframe.write \
                .format(file_format) \
                .mode("append") \
                .option("path", output_path) \
                .save()
            logger.debug("Written the file- %s", output_path)
            return "Success"
        except Exception as err:
            logger.error("%s, Error: %s", str(__name__), str(err))