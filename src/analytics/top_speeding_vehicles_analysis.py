from pyspark.sql.functions import col,row_number
from pyspark.sql import Window
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
        :param files: Dictionary Object config['input']
        :return:  Returns a : Dataframe
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

        charge_speed_df = charges_df.filter(col('CHARGE').contains('SPEED')).select('CRASH_ID','UNIT_NBR','CHARGE') #charge corresponding to speed issues
        top_clrs_agg_df = units_df.filter("VEH_COLOR_ID != 'NA'").select('VEH_COLOR_ID').groupBy('VEH_COLOR_ID').count()
        top_clrs_df = top_clrs_agg_df.withColumn('rn',row_number().over(Window.orderBy(col('count').desc()))).filter("rn <=10").select('VEH_COLOR_ID')#top10 vehicle colors used
        top_states_agg = units_df.filter(~col('VEH_LIC_STATE_ID').isin('NA','Unknown','Other','UN')).select('VEH_LIC_STATE_ID').groupBy('VEH_LIC_STATE_ID').count()
        top_states_df = top_states_agg.withColumn('rn',row_number().over(Window.orderBy(col('count').desc()))).filter("rn <=25").select('VEH_LIC_STATE_ID')
        unit_sub_join = units_df.join(top_clrs_df,on=['VEH_COLOR_ID'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_LIC_STATE_ID')
        charge_sub_join = unit_sub_join.join(charge_speed_df,on=['CRASH_ID','UNIT_NBR'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_LIC_STATE_ID')
        combine_join = charge_sub_join.join(top_states_df,on=['VEH_LIC_STATE_ID'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID')
        licensed_person_df = person_df.filter("DRVR_LIC_CLS_ID != 'UNLICENSED'").select('CRASH_ID','UNIT_NBR')
        final_temp_df = combine_join.join(licensed_person_df,on=['CRASH_ID','UNIT_NBR'],how='inner').select('CRASH_ID','VEH_MAKE_ID').groupBy('VEH_MAKE_ID').count()
        result_df=final_temp_df.withColumn('rn',row_number().over(Window.orderBy(col('count').desc()))).filter("rn <= 5").select('VEH_MAKE_ID')
        return result_df

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: config['input']
        :return: Dataframe
        """
        return TopVehicleMakerschargedSpeeding.__process(TopVehicleMakerschargedSpeeding, session, files)