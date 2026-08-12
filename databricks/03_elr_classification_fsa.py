%md
# ELR classification extract — by raw FSA

Same output as the `ter_onlvl_ibc_no` version (ADIDO 2569, `ELR_Inf_<var>`): on-level term
premium and GA2M predicted loss cost summed by company x variable, then the expected loss
ratios `LR_*` = LC / premium. The only change is the grouping variable:
`ter_onlvl_ibc_no` -> raw FSA.

The header of this notebook was not included in the issue thread; it is reconstructed to
match the inforce extract, which reads the same table with the same widgets.

from pyspark.sql import functions as F
from functools import reduce
as_of_date = dbutils.widgets.get("as_of_date")
province = dbutils.widgets.get("province")
# ---------------------------------------------------------------------
# Read data
# ---------------------------------------------------------------------
df_inf = spark.sql(f"""
        select *
        from t_ap_ppa_pricing.inf_{province.lower()}_ppa_prep_{as_of_date}_onlvl
        """)
%run ./00_fsa_common
# ---------------------------------------------------------------------
# Grouping variable
# ---------------------------------------------------------------------
# WAS: VARS = ["ter_onlvl_ibc_no"]
VARS = [FSA_VAR]

#################### Interaction Variables ###############
# dri_type_cd_x_number_au_mh_x_veh_dri_onp_nb
# dri_type_cd_x_rat_km_work_nb
# dri_type_cd_x_rat_km_business_nb
# dri_type_cd_x_rat_km_annual_nb
# "ter_onlvl_ibc_no"

# ---------------------------------------------------------------------
# Columns used in aggregation
# ---------------------------------------------------------------------
rename_map = {
    "Prm_Trm_BI_Uncap_Am": "Prm_OnLvl_BI_Uncap_Am",
    "Prm_Trm_PD_Uncap_Am": "Prm_OnLvl_PD_Uncap_Am",
    "Prm_Trm_DC_Uncap_Am": "Prm_OnLvl_DC_Uncap_Am",
    "Prm_Trm_AB_Uncap_Am": "Prm_OnLvl_AB_Uncap_Am",
    "Prm_Trm_UA_Uncap_Am": "Prm_OnLvl_UM_Uncap_Am",
    "Prm_Trm_AP_SP_CMP_Uncap_Am": "Prm_OnLvl_AP_SP_CMP_Uncap_Am",
    "Prm_Trm_AP_COL_Uncap_Am": "Prm_OnLvl_AP_COL_Uncap_Am",
    "Prm_Trm_SP_Uncap_Am": "Prm_OnLvl_SPE_Uncap_Am",
    "Prm_Trm_UI_Uncap_Am": "Prm_OnLvl_UI_Uncap_Am",
    "Prm_Trm_Dri_Tot_Uncap_Am": "Prm_OnLvl_Dri_Tot_Uncap_Am"
}

lc_cols = ["LC_BI", "LC_PD", "LC_DC", "LC_AB", "LC_COL", "LC_CMP", "LC_TOT"]

premium_cols = [
    "Prm_OnLvl_BI_Uncap_Am",
    "Prm_OnLvl_PD_Uncap_Am",
    "Prm_OnLvl_DC_Uncap_Am",
    "Prm_OnLvl_AB_Uncap_Am",
    "Prm_OnLvl_AP_COL_Uncap_Am",
    "Prm_OnLvl_AP_SP_CMP_Uncap_Am",
    "Prm_OnLvl_Dri_Tot_Uncap_Am"
]

%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_Classification
# 1) Rename columns (R: rename())
#
for old, new in rename_map.items():
    df_inf = df_inf.withColumnRenamed(old, new)

# ---------------------------------------------------------------------
# 2) If no premium, set LC to 0 (R: ifelse(is.na(premium),0,LC))
# ---------------------------------------------------------------------
df_inf = (df_inf
    .withColumn("LC_BI",  F.when(F.col("Prm_OnLvl_BI_Uncap_Am").isNull(), F.lit(0)).otherwise(F.col("lc_losscost_bi_pred_am")))
    .withColumn("LC_PD",  F.when(F.col("Prm_OnLvl_PD_Uncap_Am").isNull(), F.lit(0)).otherwise(F.col("lc_losscost_pd_pred_am")))
    .withColumn("LC_DC",  F.when(F.col("Prm_OnLvl_DC_Uncap_Am").isNull(), F.lit(0)).otherwise(F.col("lc_losscost_dc_pred_am")))
    .withColumn("LC_AB",  F.when(F.col("Prm_OnLvl_AB_Uncap_Am").isNull(), F.lit(0)).otherwise(F.col("lc_losscost_ab_pred_am")))
    .withColumn("LC_COL", F.when(F.col("Prm_OnLvl_AP_COL_Uncap_Am").isNull(), F.lit(0)).otherwise(F.col("lc_losscost_col_pred_am")))
    .withColumn("LC_CMP", F.when(F.col("Prm_OnLvl_AP_SP_CMP_Uncap_Am").isNull(), F.lit(0)).otherwise(F.col("lc_losscost_cmp_pred_am")))
    .withColumn("LC_TOT", F.when(F.col("Prm_OnLvl_Dri_Tot_Uncap_Am").isNull(), F.lit(0)).otherwise(F.col("lc_losscost_total_pred_am")))
)

# ---------------------------------------------------------------------
# 3) Prep: filter, company mapping, join territory, coalesce territory
# ---------------------------------------------------------------------


df_inf_prep = (df_inf
    .filter(F.col("dri_type_cd") != F.lit("OccasionalPrincipal"))
    .withColumn(
        "pol_uwcompany_cd",
        F.when(F.col("pol_uwcompany_cd") == F.lit("snic"), F.lit("SN"))
         .when(F.col("pol_uwcompany_cd") == F.lit("prim"), F.lit("PIC"))
         .otherwise(F.lit("TDHA"))
    )
)

df_inf_prep = ClassificationPrep(df_inf_prep)

# ClassificationPrep does not touch the FSA columns, so the derivation goes after it and
# the banding of every other variable is unchanged.
df_inf_prep = add_fsa(df_inf_prep)
fsa_coverage_check(df_inf_prep, "ELR")
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/Utils
# ---------------------------------------------------------------------
# Loop over VARS (same structure as R even though it's 1 var)
# ---------------------------------------------------------------------
for v in VARS:
    group_cols = ["pol_uwcompany_cd", v]
    select_cols = group_cols + premium_cols + lc_cols

    df_sel = df_inf_prep.select(*select_cols)

    agg_exprs = [F.sum(F.col(c)).alias(c) for c in (premium_cols + lc_cols)]

    df_out = (df_sel
        .groupBy(*group_cols)
        .agg(*agg_exprs)
        # LRs
        .withColumn("LR_BI",  F.when(F.col("Prm_OnLvl_BI_Uncap_Am") == 0,  F.lit(0)).otherwise(F.col("LC_BI")  / F.col("Prm_OnLvl_BI_Uncap_Am")))
        .withColumn("LR_PD",  F.when(F.col("Prm_OnLvl_PD_Uncap_Am") == 0,  F.lit(0)).otherwise(F.col("LC_PD")  / F.col("Prm_OnLvl_PD_Uncap_Am")))
        .withColumn("LR_DC",  F.when(F.col("Prm_OnLvl_DC_Uncap_Am") == 0,  F.lit(0)).otherwise(F.col("LC_DC")  / F.col("Prm_OnLvl_DC_Uncap_Am")))
        .withColumn("LR_AB",  F.when(F.col("Prm_OnLvl_AB_Uncap_Am") == 0,  F.lit(0)).otherwise(F.col("LC_AB")  / F.col("Prm_OnLvl_AB_Uncap_Am")))
        .withColumn("LR_COL", F.when(F.col("Prm_OnLvl_AP_COL_Uncap_Am") == 0, F.lit(0)).otherwise(F.col("LC_COL") / F.col("Prm_OnLvl_AP_COL_Uncap_Am")))
        .withColumn("LR_CMP", F.when(F.col("Prm_OnLvl_AP_SP_CMP_Uncap_Am") == 0, F.lit(0)).otherwise(F.col("LC_CMP") / F.col("Prm_OnLvl_AP_SP_CMP_Uncap_Am")))
        .withColumn("LR_TOT", F.when(F.col("Prm_OnLvl_Dri_Tot_Uncap_Am") == 0, F.lit(0)).otherwise(F.col("LC_TOT") / F.col("Prm_OnLvl_Dri_Tot_Uncap_Am")))
    )

    adido_out(table = df_out, ticket = 2569, filename = 'inf_ap_ppa_prep_onlvl', freeForm = f"ELR_Inf_{v}", fileformat='parquet', folder_out = f't_ap_ppa_pricing/data/{province.lower()}/classification/')
df_out.display()
%md
## Reconciliation

Premium and loss-cost totals must match the `ter_onlvl_ibc_no` run. The `LR_*` columns will
NOT match cell-for-cell — they are ratios of sums, so they are only comparable at the same
level of aggregation. Compare the company-level roll-up below, not the individual cells.
recon = (df_out
    .groupBy("pol_uwcompany_cd")
    .agg(*[F.sum(F.col(c)).alias(c) for c in (premium_cols + lc_cols)]))
recon.display()
