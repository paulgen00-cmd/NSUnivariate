%md
# Inforce classification extract — by raw FSA

Same output as the `ter_onlvl_ibc_no` version (ADIDO 2569, `Inforce_<var>`): coverage
exposure counts and term premium summed by company x variable. The only analytical change
is the grouping variable: `ter_onlvl_ibc_no` -> raw FSA.

from pyspark.sql import functions as F
from functools import reduce
as_of_date = dbutils.widgets.get("as_of_date")
province = dbutils.widgets.get("province")
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

# Source indicators the two AP_* exposures are built from.
raw_cov_cols = ["cov_bi_in", "cov_dc_in", "cov_pd_in", "cov_ab_in",
                "cov_ap_in", "cov_col_in", "cov_spe_in", "cov_cmp_in"]

cov_cols = [
    "cov_bi_in",
    "cov_dc_in",
    "cov_pd_in",
    "cov_ab_in",
    "cov_ap_col_in",
    "cov_ap_sp_cmp_in"
]
%md
## Read data

`inf_{prov}_ppa_prep_{YYYY_MM}_onlvl` is what `onlevel_premiums` writes for each province
in `04_ap_ppa_inforce_pipeline.py`, and it already carries the GA2M loss costs and the
CLEAR on-level — so the name in the original code is correct. The AP-wide table is listed
as a fallback only.
required_cols = premium_cols + raw_cov_cols + ["dri_type_cd", "pol_uwcompany_cd",
                                               "pol_jurisdiction_cd", FSA_SOURCE_COL]

df_inf, inf_table = resolve_table(
    candidates = [
        f"t_ap_ppa_pricing.inf_{province.lower()}_ppa_prep_{as_of_date}_onlvl",
        f"t_ap_ppa_pricing.inf_ap_ppa_prep_onlvl_{as_of_date}",
    ],
    required_cols = required_cols,
    context = "INFORCE",
)

# This exhibit groups by company and FSA only — there is no jurisdiction in the key, so an
# AP-wide table would silently blend four provinces into one cell. Filter, always.
df_inf = df_inf.filter(F.col("pol_jurisdiction_cd") == F.lit(province.upper()))
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

# How many rows carry a null coverage indicator? `dataprep` zeroes cov_col_in/cov_cmp_in/
# cov_spe_in when cov_ap_in = 1, so these sums stay 0/1 — but a null in either operand
# makes the whole sum null, which drops a real exposure. Coalescing to 0 matches R's
# na.rm = TRUE. If this prints 0 the coalesce is a no-op and the output is identical.
null_cov = df_inf.filter(
    reduce(lambda acc, c: acc | F.col(c).isNull(), raw_cov_cols, F.lit(False))
).count()
print(f"[INFORCE] rows with a null cov_*_in: {null_cov:,}")

df_inf = (df_inf
    .withColumn("cov_ap_col_in",
                F.coalesce(F.col("cov_ap_in"), F.lit(0)) + F.coalesce(F.col("cov_col_in"), F.lit(0)))
    .withColumn("cov_ap_sp_cmp_in",
                F.coalesce(F.col("cov_ap_in"), F.lit(0))
                + F.coalesce(F.col("cov_spe_in"), F.lit(0))
                + F.coalesce(F.col("cov_cmp_in"), F.lit(0)))
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

require_columns(df_inf_prep, premium_cols + cov_cols + VARS + ["pol_uwcompany_cd"],
                "INFORCE post-prep")
fsa_coverage_check(df_inf_prep, "INFORCE")

# The AP_* exposures must be 0/1, never 2. If dataprep's mutual-exclusion rules did not
# run on this table, a vehicle with both AP and COL would double-count.
bad_expo = df_inf_prep.filter((F.col("cov_ap_col_in") > 1) | (F.col("cov_ap_sp_cmp_in") > 1)).count()
assert bad_expo == 0, (
    f"{bad_expo:,} rows have cov_ap_col_in or cov_ap_sp_cmp_in > 1 — AP/COL/CMP/SPE are not "
    f"mutually exclusive on this table, so exposures would be double counted. Confirm the "
    f"table came through dataprep()."
)
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
    df_out.cache()

    adido_out(table = df_out, ticket = 2569, filename = 'inf_ap_ppa_prep_onlvl', freeForm = f"Inforce_{v}", fileformat='parquet', folder_out = f't_ap_ppa_pricing/data/{province.lower()}/classification/')

thin_cell_report(df_out, "Xpo_BI", "INFORCE", floor = 100)
df_out.display()
%md
## Reconciliation

Company-level totals must match the `ter_onlvl_ibc_no` run exactly — same rows, different key.
measure_out = [c for c in df_out.columns if c not in ("pol_uwcompany_cd", FSA_VAR)]

recon = (df_out
    .groupBy("pol_uwcompany_cd")
    .agg(*[F.sum(F.col(c)).alias(c) for c in measure_out]))
recon.display()
print(f"OK - source table {inf_table}, province {province.upper()}")
