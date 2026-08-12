# ============================================================================
# CELL 1 - paste alone, nothing else in the cell
# ============================================================================
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_Classification

# ============================================================================
# CELL 2 - paste alone, nothing else in the cell
# ============================================================================
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/Utils

# ============================================================================
# CELL 3 - everything below is one cell, paste it all at once
#
# ELR by EVERY available classification variable, with closing and retention
# attached to each level.
#
# Output is LONG, not one file per variable: one row per
# (company, variable_name, level_value), so all variables stack into a single
# table you can pivot.
#
# Three blocks of columns per row:
#   ELR       Prm_OnLvl_* / LC_* / LR_*        - as the original ELR pull
#   PREDICTED closing_fc / ft_retention_fc / mt_retention_fc
#             TD's production demand scores, already on every inforce row.
#             No join, always available.
#   ACTUAL    quotes / bound / closing_ratio   - from the closing view.
#             actual retention                 - only if you point RETENTION_TABLE
#                                                at a table that has it.
#
# Actual closing CANNOT be read off inforce: inforce holds policies that sold,
# so lost quotes are absent and the ratio would be sales/sales = 1. It has to
# come from the closing by-veh/dri view, aggregated to the same cell key.
# ============================================================================
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

dbutils.widgets.text("province", "NS")
dbutils.widgets.text("as_of_date", "2026_03")
dbutils.widgets.text("closing_table",
                     "prod_40tdds.tdds_ppod_actdata_auto_personal_closing_byvehdri_vw")
dbutils.widgets.text("retention_table", "")   # blank = predicted retention only
dbutils.widgets.text("output_table", "t_ap_ppa_pricing.elr_classification_demand")

province = dbutils.widgets.get("province")
as_of_date = dbutils.widgets.get("as_of_date")
CLOSING_TABLE = dbutils.widgets.get("closing_table").strip()
RETENTION_TABLE = dbutils.widgets.get("retention_table").strip()
OUTPUT_TABLE = dbutils.widgets.get("output_table").strip()

# ---------------------------------------------------------------------------
# Every classification variable. ClassificationPrep bands in place, so the
# banded value lands back on the same column name.
# Anything not on the table is skipped and reported - "all available".
# ---------------------------------------------------------------------------
CANDIDATE_VARS = [
    # driver
    "dri_type_cd", "dri_gender_cd", "dri_yrs_licensed_au_nb",
    # usage
    "rat_km_annual_nb", "rat_km_work_nb", "rat_km_business_nb",
    # household / vehicle
    "number_veh_in_family", "number_au_mh", "veh_dri_onp_nb",
    "veh_age_nb", "veh_vicc_price_am",
    "veh_rg_ab_no", "veh_rg_dc_no", "veh_rg_col_no", "veh_rg_cmp_no",
    # coverage terms
    "cov_tpl_limit_am", "cov_col_ded_am", "cov_cmp_ded_am",
    # credit
    "clt_p_holder_credit_score_no",
    # driving record
    "exp_col_af_10yrs_nb", "exp_col_af_10yrs_avg_nb",
    "exp_minor_03yrs_nb", "exp_minor_03yrs_avg_nb",
    "exp_major_03yrs_nb", "exp_criminal_03yrs_nb",
    "exp_susp_minus_03yrs_nb", "exp_susp_plus_03yrs_nb",
    "exp_canc_nopay_03yrs_nb",
    # geography
    "ter_onlvl_ibc_no", "veh_fsa_tx",
    # interactions built by ClassificationPrep
    "dri_type_cd_x_number_au_mh_x_veh_dri_onp_nb",
    "dri_type_cd_x_rat_km_work_nb",
    "dri_type_cd_x_rat_km_business_nb",
    "dri_type_cd_x_rat_km_annual_nb",
    "dri_yrs_licensed_au_nb_x_dri_gender_cd",
]

DEMAND_FC = ["rat_fulldemand_closing_fc",
             "rat_fulldemand_ft_retention_fc",
             "rat_fulldemand_mt_retention_fc"]

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

COVERAGES = [
    ("BI",  "Prm_OnLvl_BI_Uncap_Am",        "lc_losscost_bi_pred_am"),
    ("PD",  "Prm_OnLvl_PD_Uncap_Am",        "lc_losscost_pd_pred_am"),
    ("DC",  "Prm_OnLvl_DC_Uncap_Am",        "lc_losscost_dc_pred_am"),
    ("AB",  "Prm_OnLvl_AB_Uncap_Am",        "lc_losscost_ab_pred_am"),
    ("COL", "Prm_OnLvl_AP_COL_Uncap_Am",    "lc_losscost_col_pred_am"),
    ("CMP", "Prm_OnLvl_AP_SP_CMP_Uncap_Am", "lc_losscost_cmp_pred_am"),
    ("TOT", "Prm_OnLvl_Dri_Tot_Uncap_Am",   "lc_losscost_total_pred_am"),
]

lc_cols = [f"LC_{c}" for c, _, _ in COVERAGES]
premium_cols = [p for _, p, _ in COVERAGES]
lc_source_cols = [lc for _, _, lc in COVERAGES]
TOT_PRM = "Prm_OnLvl_Dri_Tot_Uncap_Am"

required_cols = list(rename_map) + lc_source_cols + ["dri_type_cd", "pol_uwcompany_cd",
                                                     "pol_jurisdiction_cd"]

caveats = []

# ---------------------------------------------------------------------------
# Inforce
# ---------------------------------------------------------------------------
df_inf = None
tried = []
for t in [f"t_ap_ppa_pricing.inf_{province.lower()}_ppa_prep_{as_of_date}_onlvl",
          f"t_ap_ppa_pricing.inf_ap_ppa_prep_onlvl_{as_of_date}"]:
    try:
        d = spark.table(t)
    except AnalysisException:
        tried.append(f"{t} -> missing table")
        continue
    gaps = [c for c in required_cols if c not in d.columns]
    if gaps:
        tried.append(f"{t} -> missing {gaps[:5]}")
        continue
    df_inf, inf_table = d, t
    break
if df_inf is None:
    raise ValueError("no usable inforce table:\n  " + "\n  ".join(tried))
print(f"inforce: {inf_table}")

df_inf = df_inf.filter(F.col("pol_jurisdiction_cd") == F.lit(province.upper()))

for old, new in rename_map.items():
    df_inf = df_inf.withColumnRenamed(old, new)

for name, prm, lc in COVERAGES:
    df_inf = df_inf.withColumn(
        f"LC_{name}",
        F.when(F.col(prm).isNull(), F.lit(0.0)).otherwise(F.col(lc))
    )

df_inf = (df_inf
    .filter(F.col("dri_type_cd") != F.lit("OccasionalPrincipal"))
    .withColumn(
        "pol_uwcompany_cd",
        F.when(F.col("pol_uwcompany_cd") == F.lit("snic"), F.lit("SN"))
         .when(F.col("pol_uwcompany_cd") == F.lit("prim"), F.lit("PIC"))
         .otherwise(F.lit("TDHA"))
    )
)

df_inf = ClassificationPrep(df_inf)

# Raw FSA, so it can be one of the variables like any other.
if "veh_fsa_tx" in df_inf.columns:
    _fsa = F.upper(F.trim(F.col("veh_fsa_tx")))
    df_inf = df_inf.withColumn(
        "veh_fsa_tx",
        F.when(_fsa.isNull() | (_fsa == F.lit("")), F.lit("UNKNOWN")).otherwise(_fsa))

VARS = [v for v in CANDIDATE_VARS if v in df_inf.columns]
skipped_vars = [v for v in CANDIDATE_VARS if v not in df_inf.columns]
print(f"\n{len(VARS)} classification variable(s) available")
if skipped_vars:
    print(f"{len(skipped_vars)} not on the inforce table, skipped: {skipped_vars}")
    caveats.append(f"variables absent from inforce: {skipped_vars}")

have_fc = [c for c in DEMAND_FC if c in df_inf.columns]
if len(have_fc) < len(DEMAND_FC):
    missing_fc = [c for c in DEMAND_FC if c not in have_fc]
    print(f"predicted demand factors missing: {missing_fc}")
    caveats.append(f"predicted demand factors missing: {missing_fc}")

# ---------------------------------------------------------------------------
# Closing view -> actual closing per cell
#
# Counted on DISTINCT pol_quote_no, never count(*): the by-veh/dri view repeats
# one quote across vehicle x driver rows, and multi-vehicle households repeat
# more than single-vehicle ones. A multi-vehicle quote can also land in more
# than one level of the same variable, so cells do NOT sum to the book total.
# ---------------------------------------------------------------------------
df_clo = None
clo_vars = []
if CLOSING_TABLE:
    try:
        df_clo = spark.table(CLOSING_TABLE)
    except AnalysisException:
        print(f"\nclosing view {CLOSING_TABLE} not readable - actual closing skipped")
        caveats.append(f"closing view {CLOSING_TABLE} not readable")

if df_clo is not None:
    need = ["pol_quote_no", "clo_bound_in", "pol_jurisdiction_cd"]
    gaps = [c for c in need if c not in df_clo.columns]
    if gaps:
        print(f"\nclosing view missing {gaps} - actual closing skipped")
        caveats.append(f"closing view missing {gaps}")
        df_clo = None

if df_clo is not None:
    df_clo = df_clo.filter(F.col("pol_jurisdiction_cd") == F.lit(province.upper()))
    for c, val in [("veh_product_cd", "ppa"), ("pol_commercial_in", 0)]:
        if c in df_clo.columns:
            df_clo = df_clo.filter(F.col(c) == F.lit(val))

    # The closing view does NOT go through the AP dataprep, so the derived
    # rating variables are absent. Recreate the two ClassificationPrep needs.
    if "number_au_mh" not in df_clo.columns and \
       all(c in df_clo.columns for c in ("veh_product_cd", "clt_veh_mh_nb", "clt_veh_au_nb")):
        df_clo = df_clo.withColumn(
            "number_au_mh",
            F.when(F.col("veh_product_cd") == F.lit("motor_home"),
                   F.col("clt_veh_mh_nb")).otherwise(F.col("clt_veh_au_nb")))
    if "number_veh_in_family" not in df_clo.columns and \
       all(c in df_clo.columns for c in ("clt_veh_au_nb", "clt_veh_mh_nb")):
        df_clo = df_clo.withColumn(
            "number_veh_in_family",
            F.coalesce(F.col("clt_veh_au_nb"), F.lit(0)) + F.coalesce(F.col("clt_veh_mh_nb"), F.lit(0)))

    if "pol_uwcompany_cd" in df_clo.columns:
        df_clo = df_clo.withColumn(
            "pol_uwcompany_cd",
            F.when(F.col("pol_uwcompany_cd") == F.lit("snic"), F.lit("SN"))
             .when(F.col("pol_uwcompany_cd") == F.lit("prim"), F.lit("PIC"))
             .otherwise(F.lit("TDHA")))
    else:
        df_clo = df_clo.withColumn("pol_uwcompany_cd", F.lit("TDHA"))
        caveats.append("closing view has no pol_uwcompany_cd - all quotes booked to TDHA")

    # Band with the SAME function as the inforce side. Two banding
    # implementations drifting apart is how this exhibit goes quietly wrong.
    try:
        df_clo = ClassificationPrep(df_clo)
    except Exception as e:
        print(f"\nClassificationPrep failed on the closing view: {e}")
        caveats.append(f"ClassificationPrep failed on closing view: {e}")
        df_clo = None

if df_clo is not None:
    if "veh_fsa_tx" in df_clo.columns:
        _f = F.upper(F.trim(F.col("veh_fsa_tx")))
        df_clo = df_clo.withColumn(
            "veh_fsa_tx",
            F.when(_f.isNull() | (_f == F.lit("")), F.lit("UNKNOWN")).otherwise(_f))
    clo_vars = [v for v in VARS if v in df_clo.columns]
    print(f"\nactual closing available for {len(clo_vars)} of {len(VARS)} variable(s)")
    no_clo = [v for v in VARS if v not in clo_vars]
    if no_clo:
        caveats.append(f"no actual closing for: {no_clo}")

# ---------------------------------------------------------------------------
# Optional actual retention
# ---------------------------------------------------------------------------
df_ret = None
ret_vars = []
RET_KEY = "pol_policy_no"
if RETENTION_TABLE:
    try:
        df_ret = spark.table(RETENTION_TABLE)
    except AnalysisException:
        print(f"\nretention table {RETENTION_TABLE} not readable - actual retention skipped")
        caveats.append(f"retention table {RETENTION_TABLE} not readable")
    if df_ret is not None:
        need = ["retained_in", RET_KEY]
        gaps = [c for c in need if c not in df_ret.columns]
        if gaps:
            print(f"\nretention table missing {gaps} - actual retention skipped")
            caveats.append(f"retention table missing {gaps}")
            df_ret = None
    if df_ret is not None:
        if "pol_jurisdiction_cd" in df_ret.columns:
            df_ret = df_ret.filter(F.col("pol_jurisdiction_cd") == F.lit(province.upper()))
        if "pol_uwcompany_cd" not in df_ret.columns:
            df_ret = df_ret.withColumn("pol_uwcompany_cd", F.lit("TDHA"))
        ret_vars = [v for v in VARS if v in df_ret.columns]
        print(f"\nactual retention available for {len(ret_vars)} of {len(VARS)} variable(s)")
else:
    print("\nretention_table blank - predicted retention factors only")
    caveats.append("no actual retention source supplied; ft/mt columns are PREDICTED scores")

# ---------------------------------------------------------------------------
# Build one long frame
# ---------------------------------------------------------------------------
NULL_D = F.lit(None).cast("double")

def wtd_mean(value_col, weight_col):
    """Premium-weighted mean, null-safe. Falls back to null on zero weight."""
    num = F.sum(F.coalesce(F.col(value_col), F.lit(0.0)) * F.coalesce(F.col(weight_col), F.lit(0.0)))
    den = F.sum(F.when(F.col(value_col).isNull(), F.lit(0.0))
                 .otherwise(F.coalesce(F.col(weight_col), F.lit(0.0))))
    return num, den

parts = []
for v in VARS:
    aggs = [F.sum(F.col(c)).alias(c) for c in (premium_cols + lc_cols)]
    aggs.append(F.count(F.lit(1)).alias("vehicles"))

    for fc in DEMAND_FC:
        short = fc.replace("rat_fulldemand_", "").replace("_fc", "")
        if fc in have_fc:
            aggs.append(F.avg(F.col(fc)).alias(f"pred_{short}"))
            num, den = wtd_mean(fc, TOT_PRM)
            aggs.append(num.alias(f"_num_{short}"))
            aggs.append(den.alias(f"_den_{short}"))
        else:
            aggs.append(NULL_D.alias(f"pred_{short}"))
            aggs.append(NULL_D.alias(f"_num_{short}"))
            aggs.append(NULL_D.alias(f"_den_{short}"))

    g = (df_inf
         .withColumn("level_value", F.col(v).cast("string"))
         .groupBy("pol_uwcompany_cd", "level_value")
         .agg(*aggs)
         .withColumn("variable_name", F.lit(v)))

    for name, prm, _ in COVERAGES:
        g = g.withColumn(
            f"LR_{name}",
            F.when(F.col(prm).isNull() | (F.col(prm) == 0), F.lit(0.0))
             .otherwise(F.col(f"LC_{name}") / F.col(prm)))

    for fc in DEMAND_FC:
        short = fc.replace("rat_fulldemand_", "").replace("_fc", "")
        g = g.withColumn(
            f"pred_{short}_prem_wtd",
            F.when(F.col(f"_den_{short}").isNull() | (F.col(f"_den_{short}") == 0), NULL_D)
             .otherwise(F.col(f"_num_{short}") / F.col(f"_den_{short}")))
        g = g.drop(f"_num_{short}").drop(f"_den_{short}")

    # actual closing for this variable
    if v in clo_vars:
        c = (df_clo
             .withColumn("level_value", F.col(v).cast("string"))
             .groupBy("pol_uwcompany_cd", "level_value")
             .agg(F.countDistinct(F.col("pol_quote_no")).alias("quotes"),
                  F.countDistinct(F.when(F.coalesce(F.col("clo_bound_in"), F.lit(0)) == 1,
                                         F.col("pol_quote_no"))).alias("bound")))
        g = g.join(c, on=["pol_uwcompany_cd", "level_value"], how="left")
        g = g.withColumn(
            "closing_ratio",
            F.when(F.col("quotes").isNull() | (F.col("quotes") == 0), NULL_D)
             .otherwise(F.col("bound") / F.col("quotes")))
    else:
        g = (g.withColumn("quotes", F.lit(None).cast("long"))
              .withColumn("bound", F.lit(None).cast("long"))
              .withColumn("closing_ratio", NULL_D))

    # actual retention for this variable
    if v in ret_vars:
        r = (df_ret
             .withColumn("level_value", F.col(v).cast("string"))
             .groupBy("pol_uwcompany_cd", "level_value")
             .agg(F.countDistinct(F.col(RET_KEY)).alias("policies_exposed"),
                  F.countDistinct(F.when(F.coalesce(F.col("retained_in"), F.lit(0)) == 1,
                                         F.col(RET_KEY))).alias("policies_retained")))
        g = g.join(r, on=["pol_uwcompany_cd", "level_value"], how="left")
        g = g.withColumn(
            "retention_actual",
            F.when(F.col("policies_exposed").isNull() | (F.col("policies_exposed") == 0), NULL_D)
             .otherwise(F.col("policies_retained") / F.col("policies_exposed")))
    else:
        g = (g.withColumn("policies_exposed", F.lit(None).cast("long"))
              .withColumn("policies_retained", F.lit(None).cast("long"))
              .withColumn("retention_actual", NULL_D))

    parts.append(g)

df_out = parts[0]
for p in parts[1:]:
    df_out = df_out.unionByName(p)

ORDER = (["variable_name", "level_value", "pol_uwcompany_cd", "vehicles"]
         + premium_cols + lc_cols + [f"LR_{c}" for c, _, _ in COVERAGES]
         + ["pred_closing", "pred_closing_prem_wtd",
            "pred_ft_retention", "pred_ft_retention_prem_wtd",
            "pred_mt_retention", "pred_mt_retention_prem_wtd",
            "quotes", "bound", "closing_ratio",
            "policies_exposed", "policies_retained", "retention_actual"])
df_out = df_out.select(*[c for c in ORDER if c in df_out.columns])

print("\n" + "=" * 70)
print(f"{len(VARS)} variable(s) stacked")
if caveats:
    print("CAVEATS:")
    for c in caveats:
        print(f"  - {c}")
print("=" * 70)

if OUTPUT_TABLE:
    spark.sql(f"drop table if exists {OUTPUT_TABLE}")
    df_out.write.format("delta").mode("overwrite").saveAsTable(OUTPUT_TABLE)
    print(f"written to {OUTPUT_TABLE}")

adido_out(table = df_out, ticket = 2569, filename = 'inf_ap_ppa_prep_onlvl', freeForm = "ELR_Demand_AllVars", fileformat='parquet', folder_out = f't_ap_ppa_pricing/data/{province.lower()}/classification/')

df_out.display()
