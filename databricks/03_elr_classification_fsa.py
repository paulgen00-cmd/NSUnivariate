%md
# ELR classification extract — by raw FSA

Same output as the `ter_onlvl_ibc_no` version (ADIDO 2569, `ELR_Inf_<var>`): on-level term
premium and GA2M predicted loss cost summed by company x variable, then the expected loss
ratios `LR_*` = LC / premium. The only analytical change is the grouping variable:
`ter_onlvl_ibc_no` -> raw FSA.

The header of this notebook was not included in the issue thread; it is reconstructed to
match the inforce extract, which reads the same table with the same widgets.

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

lc_source_cols = [
    "lc_losscost_bi_pred_am", "lc_losscost_pd_pred_am", "lc_losscost_dc_pred_am",
    "lc_losscost_ab_pred_am", "lc_losscost_col_pred_am", "lc_losscost_cmp_pred_am",
    "lc_losscost_total_pred_am"
]

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

# The seven (premium, loss cost, output LR) triples, so the LC zeroing and the LR
# divisions below cannot drift apart from each other.
COVERAGES = [
    ("BI",  "Prm_OnLvl_BI_Uncap_Am",         "lc_losscost_bi_pred_am"),
    ("PD",  "Prm_OnLvl_PD_Uncap_Am",         "lc_losscost_pd_pred_am"),
    ("DC",  "Prm_OnLvl_DC_Uncap_Am",         "lc_losscost_dc_pred_am"),
    ("AB",  "Prm_OnLvl_AB_Uncap_Am",         "lc_losscost_ab_pred_am"),
    ("COL", "Prm_OnLvl_AP_COL_Uncap_Am",     "lc_losscost_col_pred_am"),
    ("CMP", "Prm_OnLvl_AP_SP_CMP_Uncap_Am",  "lc_losscost_cmp_pred_am"),
    ("TOT", "Prm_OnLvl_Dri_Tot_Uncap_Am",    "lc_losscost_total_pred_am"),
]
%md
## Read data

Same table as the inforce extract. `Prm_Trm_Dri_Tot_Uncap_Am` is the LR_TOT denominator and
is the one column in `rename_map` this repo could not confirm against a schema dump — if
your scored table exposes the driver total under a different name, the preflight check below
names it rather than letting the rename silently do nothing.
required_cols = list(rename_map) + lc_source_cols + ["dri_type_cd", "pol_uwcompany_cd",
                                                     "pol_jurisdiction_cd", FSA_SOURCE_COL]

df_inf, inf_table = resolve_table(
    candidates = [
        f"t_ap_ppa_pricing.inf_{province.lower()}_ppa_prep_{as_of_date}_onlvl",
        f"t_ap_ppa_pricing.inf_ap_ppa_prep_onlvl_{as_of_date}",
    ],
    required_cols = required_cols,
    context = "ELR",
)

# No jurisdiction in the group-by, so an AP-wide table would blend four provinces.
df_inf = df_inf.filter(F.col("pol_jurisdiction_cd") == F.lit(province.upper()))
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_Classification
# 1) Rename columns (R: rename())
#
# withColumnRenamed is a silent no-op on a column that does not exist, so verify first.
require_columns(df_inf, list(rename_map), "ELR pre-rename")

for old, new in rename_map.items():
    df_inf = df_inf.withColumnRenamed(old, new)

require_columns(df_inf, premium_cols, "ELR post-rename")

# ---------------------------------------------------------------------
# 2) If no premium, set LC to 0 (R: ifelse(is.na(premium),0,LC))
# ---------------------------------------------------------------------
for name, prm, lc in COVERAGES:
    df_inf = df_inf.withColumn(
        f"LC_{name}",
        F.when(F.col(prm).isNull(), F.lit(0.0)).otherwise(F.col(lc))
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

require_columns(df_inf_prep, premium_cols + lc_cols + VARS + ["pol_uwcompany_cd"],
                "ELR post-prep")
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
    )

    # LRs. The original guarded premium == 0; a null premium sum (every row in the cell
    # null) fell through the guard and produced a null LR next to a zeroed LC. Treat null
    # the same as zero so the two cases agree.
    for name, prm, _ in COVERAGES:
        df_out = df_out.withColumn(
            f"LR_{name}",
            F.when(F.col(prm).isNull() | (F.col(prm) == 0), F.lit(0.0))
             .otherwise(F.col(f"LC_{name}") / F.col(prm))
        )

    df_out.cache()

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

for name, prm, _ in COVERAGES:
    recon = recon.withColumn(
        f"LR_{name}",
        F.when(F.col(prm).isNull() | (F.col(prm) == 0), F.lit(0.0))
         .otherwise(F.col(f"LC_{name}") / F.col(prm))
    )

recon.display()
print(f"OK - source table {inf_table}, province {province.upper()}")
