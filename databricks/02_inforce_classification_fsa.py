%md
# Inforce classification extract — by raw FSA

Same output as the `ter_onlvl_ibc_no` version (ADIDO 2569, `Inforce_<var>`): coverage
exposure counts and term premium summed by company x variable. The only change is the
grouping variable: `ter_onlvl_ibc_no` -> raw FSA.

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
# dri_type_cd_x_number_au_mh_x_veh_dri_onp_nb,
# dri_type_cd_x_rat_km_work_nb,
# dri_type_cd_x_rat_km_business_nb,
# dri_type_cd_x_rat_km_annual_nb,
# "ter_onlvl_ibc_no"

# ---------------------------------------------------------------------
# Columns used in filters + aggregations (kept explicit for reliability)
# ---------------------------------------------------------------------
premium_cols = [
    "Prm_Trm_BI_Uncap_Am",
    "Prm_Trm_DC_Uncap_Am",
    "Prm_Trm_PD_Uncap_Am",
    "Prm_Trm_AB_Uncap_Am",
    "Prm_Trm_AP_COL_Uncap_Am",
    "Prm_Trm_AP_SP_CMP_Uncap_Am"
]

cov_cols = [
    "cov_bi_in",
    "cov_dc_in",
    "cov_pd_in",
    "cov_ab_in",
    "cov_ap_col_in",
    "cov_ap_sp_cmp_in"
]
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_Classification
# ---------------------------------------------------------------------
# Build filter condition: dri_type_cd != "OccasionalPrincipal"
# and all premium cols not null
# ---------------------------------------------------------------------
not_null_all_premiums = reduce(
    lambda acc, c: acc & F.col(c).isNotNull(),
    premium_cols,
    F.lit(True)
)

# ---------------------------------------------------------------------
# Prep data (filter, mutate, join, coalesce)
# ---------------------------------------------------------------------


df_inf = (df_inf
    .withColumn("cov_ap_col_in", F.col("cov_ap_in") + F.col("cov_col_in"))
    .withColumn("cov_ap_sp_cmp_in", F.col("cov_ap_in") + F.col("cov_spe_in") + F.col("cov_cmp_in"))
)

# ---------------------------------------------------------------------
# Prep: filter, company mapping, join territory, coalesce territory
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
fsa_coverage_check(df_inf_prep, "INFORCE")
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/Utils
# ---------------------------------------------------------------------
# Loop over VARS (same structure as R even though it's 1 var)
# ---------------------------------------------------------------------
for v in VARS:
    group_cols = ["pol_uwcompany_cd", v]

    df_sel = df_inf_prep.select(*(group_cols + premium_cols + cov_cols))

    df_out = (df_sel
        .groupBy(*group_cols)
        .agg(
            # Exposures (R: Xpo_* = sum(cov_*_in, na.rm=T))
            F.sum("cov_bi_in").alias("Xpo_BI"),
            F.sum("cov_dc_in").alias("Xpo_DC"),
            F.sum("cov_pd_in").alias("Xpo_PD"),
            F.sum("cov_ab_in").alias("Xpo_AB"),
            F.sum("cov_ap_col_in").alias("Xpo_AP_COL"),
            F.sum("cov_ap_sp_cmp_in").alias("Xpo_AP_SP_CMP"),

            # Premiums (R: sum(Prm_Trm_*, na.rm=T))
            F.sum("Prm_Trm_BI_Uncap_Am").alias("Prm_Trm_BI_Uncap_Am"),
            F.sum("Prm_Trm_DC_Uncap_Am").alias("Prm_Trm_DC_Uncap_Am"),
            F.sum("Prm_Trm_PD_Uncap_Am").alias("Prm_Trm_PD_Uncap_Am"),
            F.sum("Prm_Trm_AB_Uncap_Am").alias("Prm_Trm_AB_Uncap_Am"),
            F.sum("Prm_Trm_AP_COL_Uncap_Am").alias("Prm_Trm_AP_COL_Uncap_Am"),
            F.sum("Prm_Trm_AP_SP_CMP_Uncap_Am").alias("Prm_Trm_AP_SP_CMP_Uncap_Am")
        )
    )

    adido_out(table = df_out, ticket = 2569, filename = 'inf_ap_ppa_prep_onlvl', freeForm = f"Inforce_{v}", fileformat='parquet', folder_out = f't_ap_ppa_pricing/data/{province.lower()}/classification/')

df_out.display()
%md
## Reconciliation

Company-level totals must match the `ter_onlvl_ibc_no` run exactly — same rows, different key.
recon = (df_out
    .groupBy("pol_uwcompany_cd")
    .agg(*[F.sum(F.col(c)).alias(c) for c in df_out.columns
           if c not in ("pol_uwcompany_cd", FSA_VAR)]))
recon.display()
