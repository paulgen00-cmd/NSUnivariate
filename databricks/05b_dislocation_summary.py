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
# Same aggregates and same output column names as the original `dislocation`
# notebook. The CELL BOUNDARIES DIFFER: bucketing comes from ClassificationPrep
# -- the same function the classification run uses -- instead of the banding the
# original hand-rolls, and the FSA is selected via the FSA_COL widget. See the
# Dislocation section of the README for exactly what that changes.
# ============================================================================

# ############################################################################
# HOW TO PASTE THIS IN -- read this or the first two cells silently do nothing.
#
# A Databricks magic only works as the FIRST LINE of its cell. Paste ONLY the
# `%run ...` line into CELL 1 and CELL 2 -- NOT the comment above it. A comment
# above the magic makes Databricks treat the cell as ordinary Python, the %run
# is skipped, and nothing it should load is ever defined. The symptom is a
# NameError several cells later.
#
# CELL 3 preflights the names both %run cells should have loaded.
# ############################################################################

# --- CELL 1: paste ONLY the next line, nothing above it ---------------------

%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_Classification

# --- CELL 2: paste ONLY the next line, nothing above it ---------------------

%run /Workspace/Shared/t_ap_ppa_pricing/Functions/Utils

# ============================================================================
# CELL 3 - everything below is one cell
# ============================================================================
from functools import reduce
from operator import add

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer

# --- preflight: did both %run cells actually take? -------------------------
_NEEDS = [("ClassificationPrep", "CELL 1  AP_PPA_Classification"),
          ("test_merge_df",      "CELL 2  Utils"),
          ("adido_out",          "CELL 2  Utils")]
_gone = [f"  {name:<20} expected from {src}" for name, src in _NEEDS
         if name not in globals()]
if _gone:
    raise NameError(
        "These names are missing, so the %run cell that loads each did not take:"
        + chr(10) + chr(10).join(_gone) + """

A Databricks %run only works as the FIRST LINE of its cell. Paste ONLY the
`%run ...` line -- never the `# --- CELL n` comment above it -- then re-run
those cells from the top.""")
print("preflight ok: all shared functions loaded")

_DEFAULTS = {
    "province":              "NS",
    "RUO_Current":           "NS.PPA.20260702.json",
    "RUO_Proposed":          "NS.PPA.20261214.S8.json",
    "trx_as_of_date_suffix": "202410",
    "inf_as_of_date":        "2026-03-27",
    "schema":                "u_wuhanz5",
    "FSA_COL":               "veh_fsa_tx",   # or pol_fsa_tx
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
# The four dri_type_cd_x_* / dri_yrs_licensed_au_nb_x_* names are the interaction
# keys ClassificationPrep builds. `fsa_tx` is the normalised FSA below.
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
               "fsa_tx"]

FSA_COL = W("FSA_COL")   # veh_fsa_tx (vehicle garaging) or pol_fsa_tx (policy mailing)

RUN_ADIDO = True    # False = build and display everything, export nothing

# Match the classification notebooks (01/02): snic -> SN, prim -> PIC, else TDHA.
# The original dislocation notebook defines map_company() but never calls it, so
# its exhibits are cut by the raw codes and do not line up with the
# classification run. Applied to all four frames before any join, so the
# pol_uwcompany_cd join key stays consistent on both sides.
MAP_COMPANY_CODES = True

# 01/02 also drop OccasionalPrincipal rows. Left OFF here: dropping rows changes
# the dislocation denominators, which is a bigger change than re-bucketing.
# Turn it on only if the exhibit is meant to tie row-for-row to classification.
DROP_OCCASIONAL_PRINCIPAL = False

# ============================================================================
# CELL 4 - resolve and validate the four input tables
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

# Company mapping goes here, before anything joins on pol_uwcompany_cd, so both
# sides of every join carry the same code. Doing it later would break the join.
if MAP_COMPANY_CODES:
    def _map_company(d):
        return d.withColumn(
            "pol_uwcompany_cd",
            F.when(F.col("pol_uwcompany_cd") == F.lit("snic"), F.lit("SN"))
             .when(F.col("pol_uwcompany_cd") == F.lit("prim"), F.lit("PIC"))
             .otherwise(F.lit("TDHA")))
    DF = {k: _map_company(d) for k, d in DF.items()}
    print("pol_uwcompany_cd mapped to SN / PIC / TDHA (matches 01/02)")

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
# CELL 5 - shared helpers
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

# --- bucketing: ClassificationPrep, the same one the classification run uses -
# The original dislocation notebook hand-rolls its own km / price / veh-age /
# cap banding. ClassificationPrep does all of that AND two things the hand-rolled
# version does not:
#   * builds the dri_type_cd_x_* and dri_yrs_licensed_au_nb_x_* interaction keys
#     that c_variables groups by (the original relied on pyRate's keep_banding
#     output for these, so the exhibit was cut on pyRate's bands, not ours)
#   * bands clt_p_holder_credit_score_no (1-499 ... 850+), also in c_variables
# Using it here makes the dislocation exhibit line up with the classification
# analysis. See README -- this DOES change the cell boundaries versus the
# original dislocation output.
def classification_prep(d, label):
    """01/02's chain, minus the company map (already applied in CELL 4)."""
    if DROP_OCCASIONAL_PRINCIPAL:
        d = d.filter(F.col("dri_type_cd") != F.lit("OccasionalPrincipal"))

    d = ClassificationPrep(d)

    # Raw FSA: upper/trim, nulls and blanks to UNKNOWN so the group-by keeps
    # those rows instead of silently dropping them. Same as 01/02.
    if FSA_COL not in d.columns:
        raise ValueError(
            f"{label}: {FSA_COL!r} is not on the scored table. The TRX pipeline "
            f"selects only the columns in ap_trx_data_extract_helper.csv -- add "
            f"{FSA_COL} there and re-run 05a. Available fsa columns: "
            f"{[c for c in d.columns if 'fsa' in c.lower()]}")
    _fsa = F.upper(F.trim(F.col(FSA_COL)))
    d = d.withColumn("fsa_tx",
                     F.when(_fsa.isNull() | (_fsa == F.lit("")), F.lit("UNKNOWN"))
                      .otherwise(_fsa))

    _chk = d.agg(F.count(F.lit(1)).alias("rows"),
                 F.countDistinct(F.col("fsa_tx")).alias("cells"),
                 F.sum(F.when(F.col("fsa_tx") == "UNKNOWN", 1).otherwise(0)).alias("unknown")
                 ).collect()[0]
    print(f"{label}: rows={_chk['rows']:,}  FSA cells={_chk['cells']:,}  "
          f"UNKNOWN={_chk['unknown']:,}")

    missing = [c for c in c_variables if c not in d.columns]
    if missing:
        raise ValueError(
            f"{label}: ClassificationPrep did not produce {missing}. "
            f"c_variables must name columns it builds.")
    return d

# ============================================================================
# CELL 6 - TRX dislocation, by vehicle and by driver
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

df_trx_dri_prop = classification_prep(
    add_earned(add_earned(join_sides(df_trx_dri), "current"), "proposed"),
    "TRX by driver")

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
# CELL 7 - inforce dislocation by vehicle
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

inf_veh_binned = (classification_prep(add_rate_change_bucket(inf_veh_principal),
                                      "INF by vehicle")
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
# CELL 8 - export. ADIDO 2569, parquet, the same three files as the original.
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
