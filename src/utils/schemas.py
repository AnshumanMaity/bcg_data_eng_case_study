from pyspark.sql.types import *

primary_person_schema = StructType() \
    .add("CRASH_ID", IntegerType(), nullable=False) \
    .add("UNIT_NBR", IntegerType(), nullable=True) \
    .add("PRSN_NBR", IntegerType(), nullable=True) \
    .add("PRSN_TYPE_ID", StringType(), nullable=True) \
    .add("PRSN_OCCPNT_POS_ID", StringType(), nullable=True) \
    .add("PRSN_INJRY_SEV_ID", StringType(), nullable=True) \
    .add("PRSN_AGE", StringType(), nullable=True) \
    .add("PRSN_ETHNICITY_ID", StringType(), nullable=True) \
    .add("PRSN_GNDR_ID", StringType(), nullable=True) \
    .add("PRSN_EJCT_ID", StringType(), nullable=True) \
    .add("PRSN_REST_ID", StringType(), nullable=True) \
    .add("PRSN_AIRBAG_ID", StringType(), nullable=True) \
    .add("PRSN_HELMET_ID", StringType(), nullable=True) \
    .add("PRSN_SOL_FL", StringType(), nullable=True) \
    .add("PRSN_ALC_SPEC_TYPE_ID", StringType(), nullable=True) \
    .add("PRSN_ALC_RSLT_ID", StringType(), nullable=True) \
    .add("PRSN_BAC_TEST_RSLT", StringType(), nullable=True) \
    .add("PRSN_DRG_SPEC_TYPE_ID", StringType(), nullable=True) \
    .add("PRSN_DRG_RSLT_ID", StringType(), nullable=True) \
    .add("DRVR_DRG_CAT_1_ID", StringType(), nullable=True) \
    .add("PRSN_DEATH_TIME", StringType(), nullable=True) \
    .add("INCAP_INJRY_CNT", IntegerType(), nullable=True) \
    .add("NONINCAP_INJRY_CNT", IntegerType(), nullable=True) \
    .add("POSS_INJRY_CNT", IntegerType(), nullable=True) \
    .add("NON_INJRY_CNT", IntegerType(), nullable=True) \
    .add("UNKN_INJRY_CNT", IntegerType(), nullable=True) \
    .add("TOT_INJRY_CNT", IntegerType(), nullable=True) \
    .add("DEATH_CNT", IntegerType(), nullable=True) \
    .add("DRVR_LIC_TYPE_ID", StringType(), nullable=True) \
    .add("DRVR_LIC_STATE_ID", StringType(), nullable=True) \
    .add("DRVR_LIC_CLS_ID", StringType(), nullable=True) \
    .add("DRVR_ZIP", StringType(), nullable=True)

units_schema = StructType() \
    .add("CRASH_ID", IntegerType(), nullable=False) \
    .add("UNIT_NBR", IntegerType(), nullable=True) \
    .add("UNIT_DESC_ID", StringType(), nullable=True) \
    .add("VEH_PARKED_FL", StringType(), nullable=True) \
    .add("VEH_HNR_FL", StringType(), nullable=True) \
    .add("VEH_LIC_STATE_ID", StringType(), nullable=True) \
    .add("VIN", StringType(), nullable=True) \
    .add("VEH_MOD_YEAR", StringType(), nullable=True) \
    .add("VEH_COLOR_ID", StringType(), nullable=True) \
    .add("VEH_MAKE_ID", StringType(), nullable=True) \
    .add("VEH_MOD_ID", StringType(), nullable=True) \
    .add("VEH_BODY_STYL_ID", StringType(), nullable=True) \
    .add("EMER_RESPNDR_FL", StringType(), nullable=True) \
    .add("OWNR_ZIP", StringType(), nullable=True) \
    .add("FIN_RESP_PROOF_ID", StringType(), nullable=True) \
    .add("FIN_RESP_TYPE_ID", StringType(), nullable=True) \
    .add("VEH_DMAG_AREA_1_ID", StringType(), nullable=True) \
    .add("VEH_DMAG_SCL_1_ID", StringType(), nullable=True) \
    .add("FORCE_DIR_1_ID", StringType(), nullable=True) \
    .add("VEH_DMAG_AREA_2_ID", StringType(), nullable=True) \
    .add("VEH_DMAG_SCL_2_ID", StringType(), nullable=True) \
    .add("FORCE_DIR_2_ID", StringType(), nullable=True) \
    .add("VEH_INVENTORIED_FL", StringType(), nullable=True) \
    .add("VEH_TRANSP_NAME", StringType(), nullable=True) \
    .add("VEH_TRANSP_DEST", StringType(), nullable=True) \
    .add("CONTRIB_FACTR_1_ID", StringType(), nullable=True) \
    .add("CONTRIB_FACTR_2_ID", StringType(), nullable=True) \
    .add("CONTRIB_FACTR_P1_ID", StringType(), nullable=True) \
    .add("VEH_TRVL_DIR_ID", StringType(), nullable=True) \
    .add("FIRST_HARM_EVT_INV_ID", StringType(), nullable=True) \
    .add("INCAP_INJRY_CNT", IntegerType(), nullable=True) \
    .add("NONINCAP_INJRY_CNT", IntegerType(), nullable=True) \
    .add("POSS_INJRY_CNT", IntegerType(), nullable=True) \
    .add("NON_INJRY_CNT", IntegerType(), nullable=True) \
    .add("UNKN_INJRY_CNT", IntegerType(), nullable=True) \
    .add("TOT_INJRY_CNT", IntegerType(), nullable=True) \
    .add("DEATH_CNT", IntegerType(), nullable=True)

charges_schema = StructType() \
    .add("CRASH_ID", IntegerType()) \
    .add("UNIT_NBR", IntegerType()) \
    .add("PRSN_NBR", StringType()) \
    .add("CHARGE", StringType()) \
    .add("CITATION_NBR", StringType())