from pyspark.sql.functions import col,regexp_extract
from utils.utils import Utils
from utils import schemas


class TopVehicleMakerschargedSpeeding:
    """
    Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    """

    def __process(self, session, files):
        """
        Finds out Top 5 Vehicle Makes where drivers are charged with speeding related offences
        :param session: SparkSession
        :param files: Yaml config['files']
        :return:  Returns a : Int
        """
        source_path = files['inputpath']
        charges_use_csv_path = source_path + "/" + files["charges"]
        person_use_csv_path = source_path + "/" + files["person"]
        units_use_csv_path = source_path + "/" + files["units"]

        # Reading from the CSV files
        charges_df = Utils.load_csv(session=session, path=charges_use_csv_path, header=True,
                                    schema=schemas.charges_schema)

        person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)

        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)

        # Joining on charges data with person data on crash_id
        join_condition = charges_df.CRASH_ID == person_df.CRASH_ID
        join_type = "inner"

        # charges with speed related offences with driver licences
        speeding_with_licences = charges_df.join(person_df, join_condition, join_type)\
            .where((col("CHARGE").like("%SPEED%")) &
                   (col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE")) \
            .select(charges_df.CRASH_ID,
                    charges_df.CHARGE,
                    charges_df.UNIT_NBR,
                    person_df.DRVR_LIC_TYPE_ID,
                    person_df.DRVR_LIC_STATE_ID,
                    person_df.DRVR_LIC_CLS_ID
                    )

        # Top states with highest number of offences
        top_states_offences = person_df\
            .groupBy("DRVR_LIC_STATE_ID")\
            .count()\
            .orderBy(col("count").desc()).take(25)

        # Top used vehicles colours with licenced
        top_licensed_vehicle_colors = units_df.\
            join(person_df, units_df.CRASH_ID == person_df.CRASH_ID, "inner") \
            .where((col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE")
                   & (col("DRVR_LIC_CLS_ID").like("CLASS C%"))) \
            .groupBy("VEH_COLOR_ID")\
            .count()\
            .orderBy(col("count").desc()).take(10)

        top_colors = [i["VEH_COLOR_ID"] for i in top_licensed_vehicle_colors]
        top_states = [i['DRVR_LIC_STATE_ID'] for i in top_states_offences]

        top_vehicles_made = speeding_with_licences\
            .join(units_df, speeding_with_licences.CRASH_ID == units_df.CRASH_ID, "inner") \
            .where((col("VEH_COLOR_ID").isin(top_colors))
                   & (col("DRVR_LIC_STATE_ID").isin(top_states))) \
            .select("VEH_MAKE_ID")

        # return top_vehicles_made.groupBy('VEH_MAKE_ID').(count(*).allias('cnt')).orderBy('cnt').limit(5)
        return top_vehicles_made.groupBy('VEH_MAKE_ID').count().alias('count').orderBy('count', ascending=False).limit(5)

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Integer -> Total No of crashes
        """
        return TopVehicleMakerschargedSpeeding.__process(TopVehicleMakerschargedSpeeding, session, files)