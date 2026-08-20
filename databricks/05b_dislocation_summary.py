# ============================================================================
# 05b_dislocation_summary.py -- build the three dislocation exhibits.
#
# Reads the four tables 05a_dislocation_score.py wrote. Calls pyRate ZERO
# times, so it runs in minutes and can be re-run freely.
#
# Inputs (all four must exist -- run 05a four times first):
#     trx_<prov>_ppa_prep_onlvl_<suffix>_<CUR>
#     trx_<prov>_ppa_prep_onlvl_<suffix>_<PRP>
#     inf_<prov>_ppa_prep_<f3>_<CUR>_onlvl
#     inf_<prov>_ppa_prep_<f3>_<PRP>_onlvl
#
# Outputs -- ADIDO 2569, parquet, t_ap_ppa_pricing/data/<prov>/dislocation/
#     TRX_by_Vehicle_<CUR>-<PRP>
#     TRX_by_Driver_<CUR>-<PRP>
#     Inf_by_Vehicle_<CUR>-<PRP>
#
# Same grouping, same aggregates, same column names as the original
# `dislocation` notebook. tests/test_dislocation_equivalence.py checks the
# generated column lists against the literals in that file.
# ============================================================================

# ============================================================================
# CELL 1 - paste alone
# ============================================================================
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/Utils

# ============================================================================
# CELL 2 - everything below is one cell
# ============================================================================
from functools import reduce
from operator import add

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer

_DEFAULTS = {
    "province":              "NS",
    "RUO_Current":           "NS.PPA.20260702.json",
    "RUO_Proposed":          "NS.PPA.20261214.S8.json",
    "trx_as_of_date_suffix": "202410",
    "inf_as_of_date":        "2026-03-27",
    "schema":                "u_wuhanz5",
}
for _n, _d in _DEFAULTS.items():
    dbutils.widgets.text(_n, _d)

def W(name):
    v = dbutils.widgets.get(name).strip()
    return v if v else _DEFAULTS[name]

province          = W("province").upper()
as_of_date_suffix = W("trx_as_of_date_suffix")
inf_as_of_date    = W("inf_as_of_date")
schema            = W("schema")
as_of_dt_f3       = inf_as_of_date[:-3].replace("-", "_")

# --- dislocation bucketing -------------------------------------------------
lower_bound = -0.05
upper_bound =  0.15
bin_step    =  0.05

# --- variables the summaries are cut by ------------------------------------
c_variables = ["dri_type_cd_x_rat_km_work_nb",
               "dri_type_cd_x_rat_km_business_nb",
               "dri_type_cd_x_rat_km_annual_nb",
               "dri_yrs_licensed_au_nb_x_dri_gender_cd",
               "clt_p_holder_credit_score_no",
               "veh_vicc_price_am",
               "veh_rg_ab_no",
               "veh_rg_dc_no",
               "veh_rg_col_no",
               "veh_rg_cmp_no",
               "veh_fsa_tx"]

RUN_ADIDO = True    # False = build and display everything, export nothing

# ============================================================================
# CELL 3 - resolve and validate the four input tables
# ============================================================================
def chart_suffix(chart_name):
    """Must match 05a exactly, or the table names will not line up."""
    name = chart_name.strip()
    base = name[:-5] if name.lower().endswith(".json") else name
    return base.replace(".", "_")

CUR = chart_suffix(W("RUO_Current"))
PRP = chart_suffix(W("RUO_Proposed"))
if CUR == PRP:
    raise ValueError("RUO_Current and RUO_Proposed resolve to the same CHART suffix.")

TABLES = {
    ("trx", "current"):  f"{schema}.trx_{province.lower()}_ppa_prep_onlvl_{as_of_date_suffix}_{CUR}",
    ("trx", "proposed"): f"{schema}.trx_{province.lower()}_ppa_prep_onlvl_{as_of_date_suffix}_{PRP}",
    ("inf", "current"):  f"{schema}.inf_{province.lower()}_ppa_prep_{as_of_dt_f3}_{CUR}_onlvl",
    ("inf", "proposed"): f"{schema}.inf_{province.lower()}_ppa_prep_{as_of_dt_f3}_{PRP}_onlvl",
}

_missing = [(k, t) for k, t in TABLES.items() if not spark.catalog.tableExists(t)]
if _missing:
    lines = ["Missing input table(s). Run 05a_dislocation_score.py for each:"]
    for (ds, side), t in _missing:
        chart = W("RUO_Current") if side == "current" else W("RUO_Proposed")
        lines.append(f"  {t}")
        lines.append(f"      -> 05a with dataset={ds}  RUO_Chart={chart}")
    raise ValueError("\n".join(lines))

DF = {k: spark.table(t) for k, t in TABLES.items()}

# Both sides of a dataset come from the same prep table, so their row counts
# must agree. If they do not, the two 05a runs saw different source data and
# the join below would silently drop or duplicate rows.
for ds in ("trx", "inf"):
    n_cur = DF[(ds, "current")].count()
    n_prp = DF[(ds, "proposed")].count()
    print(f"{ds}: current {n_cur:,} rows, proposed {n_prp:,} rows")
    if n_cur != n_prp:
        raise ValueError(
            f"{ds} current ({n_cur:,}) and proposed ({n_prp:,}) row counts differ. "
            f"The two 05a runs were built on different source data -- re-run both "
            f"against the same as-of date before summarising.")

# ============================================================================
# CELL 4 - shared helpers
# ============================================================================
CLAIM_COLS = ["clm_chap_bi_cap500k_am", "clm_chap_pd_am", "clm_chap_dc_am",
              "clm_chap_ab_cap500k_am", "clm_chap_um_am", "clm_chap_col_am",
              "clm_chap_cmp_am", "clm_chap_ui_am"]

# (output coverage token, source coverage token). AP_SP_CMP is written out as
# AP_SP_COMP in the summaries. Order fixes output column order.
EARN_MAP = [("BI", "BI"), ("PD", "PD"), ("DC", "DC"), ("AB", "AB"), ("UA", "UA"),
            ("AP_COL", "AP_COL"), ("AP_SP_COMP", "AP_SP_CMP"), ("UI", "UI")]

def nz(col):
    return F.coalesce(col, F.lit(0.0))

def nz_num(col, default):
    return F.coalesce(col, F.lit(default))

def nz_str(col, default="N"):
    return F.coalesce(col, F.lit(default))

def coalesce_any(df, names):
    existing = [F.col(c) for c in names if c in df.columns]
    return F.coalesce(*existing) if existing else F.lit(0.0)

def require_columns(df, cols, where):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{where}: {len(missing)} column(s) missing: {missing}")

# --- rate-change bucketing (replicates R cut(..., right = FALSE)) ----------
_n_bins  = int(round((upper_bound - lower_bound) / bin_step)) + 1
seq_vals = [lower_bound + i * bin_step for i in range(_n_bins)]
splits   = [-float("inf")] + seq_vals + [float("inf")]

def _fmt(x):
    if x == float("inf"):
        return "Inf"
    if x == -float("inf"):
        return "-Inf"
    s = f"{x:.3f}"
    return s.rstrip("0").rstrip(".") if "." in s else s

labels = [f"[{_fmt(splits[i])},{_fmt(splits[i + 1])})" for i in range(len(splits) - 1)]
print(labels)

def add_rate_change_bucket(df, col_name="Rate_Change_Pc", out_col="Rate_Change_Level"):
    bucket_col = out_col + "_idx"
    df_b = (Bucketizer(splits=splits, inputCol=col_name, outputCol=bucket_col)
            .setHandleInvalid("keep")
            .transform(df))
    labels_array = F.array(*[F.lit(l) for l in labels])
    return (df_b
            .withColumn(out_col,
                        F.when(F.col(bucket_col).isNull(), F.lit(labels[0]))
                         .otherwise(F.element_at(labels_array,
                                                 F.col(bucket_col).cast("int") + F.lit(1))))
            .drop(bucket_col))

# --- rating-variable banding ----------------------------------------------
def km_annual_group(col):
    return (F.when(col <= 5000, F.lit("0-5000"))
             .when((col > 5000) & (col <= 10000), F.lit("5001-10000"))
             .when((col > 10000) & (col <= 15000), F.lit("10001-15000"))
             .when((col > 15000) & (col <= 20000), F.lit("15001-20000"))
             .when(col > 20000, F.lit("20001+"))
             .otherwise(F.lit(None)))

def km_work_group(col):
    return (F.when(col == 0, F.lit("0"))
             .when((col > 0) & (col <= 5),   F.lit("1-5"))
             .when((col > 5) & (col <= 15),  F.lit("6-15"))
             .when((col > 15) & (col <= 30), F.lit("16-30"))
             .when(col > 30, F.lit("31+"))
             .otherwise(F.lit(None)))

def km_business_group(col):
    return F.when(col == 0, F.lit("0")).when(col > 0, F.lit("1+")).otherwise(F.lit(None))

def cap_gt8_as_str(col):
    return F.when(col > 8, F.lit(">8")).otherwise(col.cast("string"))

def cap_onp_as_str(col):
    return F.when(col > 4, F.lit(">4")).otherwise(col.cast("string"))

def veh_age_bucket(col):
    return F.when(col > 10, F.lit("10+")).otherwise(col.cast("string"))

# 5k bands from 10000 to 99999, then the three wide bands. Same 21 labels as
# the hand-written when() chain in the original.
_PRICE_BANDS = ([(10000 + 5000 * i, 15000 + 5000 * i,
                  f"{10000 + 5000 * i}-{14999 + 5000 * i}") for i in range(18)]
                + [(100000, 125000, "100000-124999"),
                   (125000, 150000, "125000-149999"),
                   (150000, 200000, "150000-199999")])

def price_band(col):
    out = F.when((col > 0) & (col < 10000), F.lit("0-9999"))
    for lo, hi, lbl in _PRICE_BANDS:
        out = out.when((col >= lo) & (col < hi), F.lit(lbl))
    return out.when(col >= 200000, F.lit("200000+")).otherwise(F.lit("Not Available"))

def band_common(d):
    """The banding block the original repeats in the TRX-driver and the
    inforce-vehicle paths."""
    return (d
            .withColumn("rat_km_annual_nb",     km_annual_group(F.col("rat_km_annual_nb")))
            .withColumn("rat_km_work_nb",       km_work_group(F.col("rat_km_work_nb")))
            .withColumn("rat_km_business_nb",   km_business_group(F.col("rat_km_business_nb")))
            .withColumn("number_veh_in_family", cap_gt8_as_str(F.col("number_veh_in_family")))
            .withColumn("number_au_mh",         cap_gt8_as_str(F.col("number_au_mh")))
            .withColumn("veh_dri_onp_nb",       cap_onp_as_str(F.col("veh_dri_onp_nb")))
            .withColumn("exp_col_af_10yrs_nb",     nz_num(F.col("exp_col_af_10yrs_nb"), 0))
            .withColumn("exp_col_af_10yrs_avg_nb", nz_num(F.col("exp_col_af_10yrs_avg_nb"), 99))
            .withColumn("exp_minor_03yrs_nb",      nz_num(F.col("exp_minor_03yrs_nb"), 0))
            .withColumn("exp_minor_03yrs_avg_nb",  nz_num(F.col("exp_minor_03yrs_avg_nb"), 99))
            .withColumn("exp_major_03yrs_nb",      nz_num(F.col("exp_major_03yrs_nb"), 0))
            .withColumn("exp_criminal_03yrs_nb",   nz_num(F.col("exp_criminal_03yrs_nb"), 0))
            .withColumn("exp_susp_minus_03yrs_nb", nz_num(F.col("exp_susp_minus_03yrs_nb"), 0))
            .withColumn("exp_susp_plus_03yrs_nb",  nz_num(F.col("exp_susp_plus_03yrs_nb"), 0))
            .withColumn("cov_cmp_ded_am", nz_str(F.col("cov_cmp_ded_am"), "N"))
            .withColumn("cov_col_ded_am", nz_str(F.col("cov_col_ded_am"), "N"))
            .withColumn("veh_age_nb",        veh_age_bucket(F.col("veh_age_nb")))
            .withColumn("veh_vicc_price_am", price_band(F.col("veh_vicc_price_am"))))

# ============================================================================
# CELL 5 - TRX dislocation, by vehicle and by driver
# ============================================================================
df_final_current  = DF[("trx", "current")]
df_final_proposed = DF[("trx", "proposed")]

uniq_keys_trx = ["pol_insurer_no", "clt_account_no", "pol_policy_no", "veh_id_no",
                 "dri_id_no", "pol_eff_dt", "trx_eff_dt", "trx_ent_dt", "pol_acc_yr_dt",
                 "trx_offset_onset_cd", "pol_jurisdiction_cd", "vehicle_driver_version_key"]

# Proposed side: "_OnLvl_Am" -> "_OnLvl_Am_Prop". Leaves "_OnLvl_End_Am" alone.
df_trx_prop = df_final_proposed
for _c in df_trx_prop.columns:
    if _c.endswith("_OnLvl_Am"):
        df_trx_prop = df_trx_prop.withColumnRenamed(_c, _c.replace("_OnLvl_Am", "_OnLvl_Am_Prop"))

prop_ol_cols = [c for c in df_trx_prop.columns if c.endswith("_OnLvl_Am_Prop")]
keep_prop    = list(dict.fromkeys(uniq_keys_trx + ["pol_uwcompany_cd"] + prop_ol_cols))
df_trx_prop1 = df_trx_prop.select(*[c for c in keep_prop if c in df_trx_prop.columns])

require_columns(df_final_current, uniq_keys_trx + ["pol_uwcompany_cd"], "TRX current")
require_columns(df_trx_prop1,     uniq_keys_trx + ["pol_uwcompany_cd"], "TRX proposed")

def add_earned(d, side):
    """Per-coverage earned premium plus the total, for one side."""
    tag   = "_Prop_Ern" if side == "proposed" else "_Ern"
    src_s = "_Prop"     if side == "proposed" else ""
    parts = []
    for out_cov, src_cov in EARN_MAP:
        out_c = f"Prm_Trm_{out_cov}_Uncap_Am{tag}"
        d = d.withColumn(out_c, nz(F.col(f"Prm_Ern_{src_cov}_OnLvl_Am{src_s}")))
        parts.append(F.col(out_c))
    return d.withColumn(f"Prm_Trm_TOT_Uncap_Am{tag}", reduce(add, parts))

def join_sides(left):
    return left.join(df_trx_prop1, on=uniq_keys_trx + ["pol_uwcompany_cd"], how="left")

df_trx_veh = add_earned(add_earned(join_sides(df_final_current), "current"), "proposed")
df_trx_veh = df_trx_veh.withColumn(
    "Clm_Tot_Am", reduce(add, [nz(F.col(c)) for c in CLAIM_COLS]))

veh_group = ["pol_acc_yr_dt", "pol_uwcompany_cd", "pol_insurer_no",
             "clt_account_no", "pol_policy_no", "veh_id_no"]

df_trx_veh_agg = (df_trx_veh.groupBy(*veh_group)
                  .agg(F.sum("Prm_Trm_TOT_Uncap_Am_Ern").alias("Prm_Trm_Veh_Base_Uncap_Am"),
                       F.sum("Prm_Trm_TOT_Uncap_Am_Prop_Ern").alias("Prm_Trm_Veh_Base_Uncap_Am_Prop"),
                       F.sum("Clm_Tot_Am").alias("clm_veh_chap_tot_am"))
                  .withColumn("Rate_Change_Pc",
                              F.when(F.col("Prm_Trm_Veh_Base_Uncap_Am") == 0, F.lit(None))
                               .otherwise(F.col("Prm_Trm_Veh_Base_Uncap_Am_Prop")
                                          / F.col("Prm_Trm_Veh_Base_Uncap_Am") - F.lit(1.0))))

df_trx_veh_binned = add_rate_change_bucket(df_trx_veh_agg)

df_trx_veh_fin = (df_trx_veh_binned
                  .groupBy("Rate_Change_Level", "pol_acc_yr_dt", "pol_uwcompany_cd")
                  .agg(F.sum("Prm_Trm_Veh_Base_Uncap_Am").alias("Prm_Trm_Veh_Base_Uncap_Am_Sum"),
                       F.sum("Prm_Trm_Veh_Base_Uncap_Am_Prop").alias("Prm_Trm_Veh_Base_Uncap_Am_Prop_Sum"),
                       F.sum("clm_veh_chap_tot_am").alias("clm_veh_chap_tot_am")))
df_trx_veh_fin.display()

# --- by driver -------------------------------------------------------------
df_trx_dri = df_final_current.join(
    df_trx_veh_binned.select(*veh_group, "Rate_Change_Level"), on=veh_group, how="left")

df_trx_dri_prop = band_common(
    add_earned(add_earned(join_sides(df_trx_dri), "current"), "proposed"))

cols_to_keep_dri = ["Rate_Change_Level", "pol_acc_yr_dt", "pol_uwcompany_cd"] \
                   + c_variables + ["pol_business_cd"]

# (source token, output token). AP_COL/AP_SP_COMP are summarised as COL/CMP.
_DRI_ALIASES = [("BI", "BI"), ("PD", "PD"), ("DC", "DC"), ("AB", "AB"), ("UA", "UA"),
                ("AP_COL", "COL"), ("AP_SP_COMP", "CMP"), ("UI", "UI")]

df_trx_dri_fin2 = df_trx_dri_prop.groupBy(*cols_to_keep_dri).agg(
    *[F.sum(f"Prm_Trm_{src}_Uncap_Am_Ern").alias(f"Prm_Trm_{dst}_Uncap_Am_Sum")
      for src, dst in _DRI_ALIASES],
    F.sum("Prm_Trm_TOT_Uncap_Am_Ern").alias("Prm_Trm_Base_Uncap_Am_Sum"),
    *[F.sum(f"Prm_Trm_{src}_Uncap_Am_Prop_Ern").alias(f"Prm_Trm_{dst}_Uncap_Am_Prop_Sum")
      for src, dst in _DRI_ALIASES],
    F.sum("Prm_Trm_TOT_Uncap_Am_Prop_Ern").alias("Prm_Trm_Dri_Base_Uncap_Am_Prop_Sum"),
    *[F.sum(nz(F.col(c))).alias(c) for c in CLAIM_COLS],
    F.sum(reduce(add, [nz(F.col(c)) for c in CLAIM_COLS])).alias("clm_chap_tot_am"))
df_trx_dri_fin2.display()

# ============================================================================
# CELL 6 - inforce dislocation by vehicle
# ============================================================================
inf_cur  = DF[("inf", "current")]
inf_prop = DF[("inf", "proposed")]

for _c in inf_prop.columns:
    if _c.endswith("Uncap_Am"):
        inf_prop = inf_prop.withColumnRenamed(_c, _c.replace("Uncap_Am", "Uncap_Am_Prop"))

uniq_keys_inf = ["pol_policy_no", "veh_id_no", "dri_id_no", "pol_eff_dt"]
prop_keep = list(dict.fromkeys(
    [c for c in inf_prop.columns if c.startswith("Prm_Trm_")]
    + uniq_keys_inf + ["pol_uwcompany_cd"]))
inf_join = inf_cur.join(inf_prop.select(*prop_keep),
                        on=uniq_keys_inf + ["pol_uwcompany_cd"], how="left")

veh_keys = ["pol_uwcompany_cd", "pol_insurer_no", "clt_account_no",
            "pol_policy_no", "veh_id_no"]
w_veh = Window.partitionBy(*veh_keys)

# AP_SP_CMP vs AP_SP_COMP: the CHART decides which spelling lands in the
# scored table, so take whichever exists.
col_cmp_cur  = coalesce_any(inf_join, ["Prm_Trm_AP_SP_CMP_Uncap_Am",
                                       "Prm_Trm_AP_SP_COMP_Uncap_Am"])
col_cmp_prop = coalesce_any(inf_join, ["Prm_Trm_AP_SP_CMP_Uncap_Am_Prop",
                                       "Prm_Trm_AP_SP_COMP_Uncap_Am_Prop"])

_INF_COVS = ["BI", "PD", "DC", "AB", "UA", "AP_COL"]

inf_veh = inf_join
for _sfx in ["", "_Prop"]:
    for _cov in _INF_COVS:
        inf_veh = inf_veh.withColumn(
            f"Prm_Trm_Veh_{_cov}_Uncap_Am{_sfx}",
            F.sum(nz(F.col(f"Prm_Trm_{_cov}_Uncap_Am{_sfx}"))).over(w_veh))
    inf_veh = (inf_veh
               .withColumn(f"Prm_Trm_Veh_AP_SP_COMP_Uncap_Am{_sfx}",
                           F.sum(nz(col_cmp_prop if _sfx else col_cmp_cur)).over(w_veh))
               .withColumn(f"Prm_Trm_Veh_UI_Uncap_Am{_sfx}",
                           F.sum(nz(F.col(f"Prm_Trm_UI_Uncap_Am{_sfx}"))).over(w_veh))
               .withColumn(f"Prm_Trm_Veh_Base_Uncap_Am{_sfx}",
                           F.sum(nz(F.col(f"Prm_Trm_Dri_Base_Uncap_Am{_sfx}"))).over(w_veh)))

inf_veh = inf_veh.withColumn(
    "Rate_Change_Pc",
    F.when(F.col("Prm_Trm_Veh_Base_Uncap_Am") == 0, F.lit(None))
     .otherwise(F.col("Prm_Trm_Veh_Base_Uncap_Am_Prop")
                / F.col("Prm_Trm_Veh_Base_Uncap_Am") - F.lit(1.0)))

# NOTE: dropDuplicates picks an arbitrary row when a vehicle carries more than
# one Principal-driver row. Kept as-is to match the original -- see README.
inf_veh_principal = (inf_veh.filter(F.col("dri_type_cd") == F.lit("Principal"))
                            .dropDuplicates(veh_keys))

inf_veh_binned = (band_common(add_rate_change_bucket(inf_veh_principal))
                  .withColumn("Mandatory_Only",
                              F.when(nz(F.col("Prm_Trm_Veh_AP_COL_Uncap_Am"))
                                     + nz(F.col("Prm_Trm_Veh_AP_SP_COMP_Uncap_Am")) == 0,
                                     F.lit(1)).otherwise(F.lit(0))))

cols_to_keep_inf = ["Rate_Change_Level", "pol_uwcompany_cd", "pol_business_cd"] + c_variables
_INF_SUM_COVS = ["BI", "PD", "DC", "AB", "UA", "AP_COL", "AP_SP_COMP", "UI", "Base"]

inf_final = inf_veh_binned.groupBy(*cols_to_keep_inf).agg(
    F.count(F.lit(1)).alias("Xpo_Tot"),
    F.sum(F.when(nz(F.col("Prm_Trm_Veh_AP_COL_Uncap_Am")) > 0, F.lit(1))
           .otherwise(F.lit(0))).alias("Xpo_Col"),
    F.sum(F.when(nz(F.col("Prm_Trm_Veh_AP_SP_COMP_Uncap_Am")) > 0, F.lit(1))
           .otherwise(F.lit(0))).alias("Xpo_Cmp"),
    *[F.sum(nz(F.col(f"Prm_Trm_Veh_{c}_Uncap_Am"))).alias(f"Prm_Trm_Veh_{c}_Uncap_Am_Sum")
      for c in _INF_SUM_COVS],
    *[F.sum(nz(F.col(f"Prm_Trm_Veh_{c}_Uncap_Am_Prop"))).alias(f"Prm_Trm_Veh_{c}_Uncap_Am_Prop_Sum")
      for c in _INF_SUM_COVS])
inf_final.display()

# ============================================================================
# CELL 7 - export. ADIDO 2569, parquet, the same three files as the original.
# ============================================================================
_OUT_FOLDER = f"t_ap_ppa_pricing/data/{province.lower()}/dislocation/"
_EXPORTS = [(df_trx_veh_fin,  f"TRX_by_Vehicle_{CUR}-{PRP}"),
            (df_trx_dri_fin2, f"TRX_by_Driver_{CUR}-{PRP}"),
            (inf_final,       f"Inf_by_Vehicle_{CUR}-{PRP}")]

if RUN_ADIDO:
    for _tbl, _free in _EXPORTS:
        adido_out(table=_tbl, ticket=2569, filename="inf_ap_ppa_prep_onlvl",
                  freeForm=_free, fileformat="parquet", folder_out=_OUT_FOLDER)
        print(f"exported {_free}")
else:
    print("RUN_ADIDO=False -- nothing exported. Would have written:")
    for _, _free in _EXPORTS:
        print("   ", _free)
