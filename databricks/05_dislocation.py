# ============================================================================
# 05_dislocation.py -- rate dislocation, current CHART vs proposed CHART
#
# Same outputs as the `dislocation` notebook at the repo root (Jessie Wu's
# original), reorganised so it is easier to run and easier to read:
#
#   * every widget self-creates with a working default, so it runs on attach
#   * the current/proposed branches are ONE code path called twice instead of
#     two hand-maintained copies (the original repeats the endorsement-split
#     block, the earning block and the 8-coverage premium block 2-3x each)
#   * on-levelling is guarded -- a failed pyRate job raises where it happens
#     instead of returning None and blowing up later inside test_merge_df
#   * RESUME_SCORED lets a re-run skip on-levels already on disk, so a crash
#     in part 2 costs seconds instead of another ~30 minutes
#   * the two different `dataprep` functions (TRX vs inforce) are captured
#     under distinct names, so the second %run can no longer shadow the first
#
# THREE OUTPUTS, same schema and grouping as the original:
#   ADIDO 2569, parquet, t_ap_ppa_pricing/data/{prov}/dislocation/
#     TRX_by_Vehicle_{current}-{proposed}   <- df_trx_veh_fin
#     TRX_by_Driver_{current}-{proposed}    <- df_trx_dri_fin2
#     Inf_by_Vehicle_{current}-{proposed}   <- inf_final
#   plus the two on-levelled TRX delta tables the original also writes.
#
# Runtime is unchanged on a cold run (~30 min, four pyRate scorings).
# ============================================================================

# ============================================================================
# CELL 1 - paste alone, nothing else in the cell
# ============================================================================
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/Utils

# ============================================================================
# CELL 2 - paste alone, nothing else in the cell
# ============================================================================
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_Onlevel

# ============================================================================
# CELL 3 - paste alone, nothing else in the cell
# ============================================================================
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_Endorsement_Split

# ============================================================================
# CELL 4 - paste alone, nothing else in the cell
# ============================================================================
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_DataPrep

# ============================================================================
# CELL 5 - capture the TRX dataprep BEFORE the inforce %run rebinds the name
# ============================================================================
dataprep_trx = dataprep          # from AP_PPA_DataPrep

# ============================================================================
# CELL 6 - paste alone, nothing else in the cell
# ============================================================================
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_DataPrep_Inforce

# ============================================================================
# CELL 7 - capture the inforce dataprep
# ============================================================================
dataprep_inf = dataprep          # from AP_PPA_DataPrep_Inforce

# ============================================================================
# CELL 8 - paste alone, nothing else in the cell
# ============================================================================
%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_CLEAR_Onlevel

# ============================================================================
# CELL 9 - config. Everything you normally touch lives here.
# ============================================================================
from functools import reduce
from operator import add

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer
from ARRR import ARRR

# --- widgets, each with a working default so the notebook runs on attach ----
_DEFAULTS = {
    "province":              "NL",          # UPPER case
    "trx_as_of_date":        "2025-10-31",  # fiscal year end
    "trx_as_of_date_suffix": "20251031",    # yyyymmdd, used in table names
    "trx_dat_year_type":     "nov-oct",
    "inf_as_of_date":        "2025-12-31",  # same inforce used for the filing
    "RUO_Current":           "NL.PPA.20260702.json",
    "RUO_Proposed":          "NL.PPA.20260827.sbr.json",
    "schema":                "u_wuhanz5",
}
for _name, _default in _DEFAULTS.items():
    dbutils.widgets.text(_name, _default)

def W(name):
    v = dbutils.widgets.get(name).strip()
    return v if v else _DEFAULTS[name]

province          = W("province").upper()
as_of_date        = W("trx_as_of_date")
as_of_date_suffix = W("trx_as_of_date_suffix")
dat_year_type     = W("trx_dat_year_type")
inf_as_of_date    = W("inf_as_of_date")
schema            = W("schema")

# --- behaviour switches ----------------------------------------------------
# RESUME_SCORED: reuse an existing {input}_onlvl table instead of re-scoring.
# Safe because the input table name is keyed on province + as-of + CHART, so a
# table under that name can only be the output of this exact scoring. Set it
# False after a dataprep change, or when the CHART file itself was republished.
RESUME_SCORED = True
RUN_ADIDO     = True    # False = build and display everything, export nothing

# --- dislocation bucketing -------------------------------------------------
lower_bound = -0.05
upper_bound =  0.15
bin_step    =  0.05

# --- variables the by-driver / by-vehicle summaries are cut by -------------
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

# --- paths -----------------------------------------------------------------
username    = "t_ap_ppa_pricing"
az_name     = "ca0tdi0cpa0001"
abfss_root  = f"abfss://home@edaaaazep{az_name}.dfs.core.windows.net"
helper_path = f"{abfss_root}/{username}/data/mapping/"

# ============================================================================
# CELL 10 - CHART names. This is where the original silently mis-derived them.
# ============================================================================
mclient = ARRR()
mclient.update_files()
_available_charts = mclient.available_charts(product="PPA", region=province)
print("available charts:", _available_charts)

def normalize_chart(value, label):
    """CHART file name exactly as pyRate lists it. Warns when unlisted."""
    name = str(value).strip()
    if not name.lower().endswith(".json"):
        name += ".json"
    if name not in _available_charts:
        print(f"*** WARNING: {label} chart {name!r} is not in available_charts for "
              f"PPA/{province}. Scoring will fail unless pyRate resolves it anyway.")
    return name

def chart_suffix(chart_name):
    """Table-safe suffix. The original used chart[:-5], which chops five real
    characters off any value entered without the .json extension -- producing a
    truncated table name AND an unresolvable chart= argument."""
    base = chart_name[:-5] if chart_name.lower().endswith(".json") else chart_name
    return base.replace(".", "_")

RUO_chart_current  = normalize_chart(W("RUO_Current"),  "RUO_Current")
RUO_chart_proposed = normalize_chart(W("RUO_Proposed"), "RUO_Proposed")
CUR = chart_suffix(RUO_chart_current)     # was RUO_chart_current_1
PRP = chart_suffix(RUO_chart_proposed)    # was RUO_chart_proposed_1

if CUR == PRP:
    raise ValueError("RUO_Current and RUO_Proposed resolve to the same CHART -- "
                     "the current and proposed copy tables would collide.")

SIDES = {"current": (RUO_chart_current, CUR), "proposed": (RUO_chart_proposed, PRP)}
print(f"current  {RUO_chart_current}  -> {CUR}")
print(f"proposed {RUO_chart_proposed} -> {PRP}")

# ============================================================================
# CELL 11 - guarded, resumable on-levelling
# ============================================================================
def onlevel(input_table, uniq_keys, chart, label):
    """onlevel_premiums with the silent-None failure mode closed off.

    The shared onlevel_premiums polls until {input_table}_onlvl appears and its
    only exit WITH a DataFrame is the break; when the pyRate job reaches
    'Failed' first, the while condition ends the loop and it returns None with
    no exception. That None then surfaces one cell later as
    "'NoneType' object has no attribute 'count'" inside test_merge_df.
    """
    out_table = input_table + "_onlvl"

    if RESUME_SCORED and spark.catalog.tableExists(out_table):
        n = spark.table(out_table).count()
        print(f"[{label}] reusing {out_table} ({n:,} rows) -- RESUME_SCORED=True")
        return spark.table(out_table)

    if not spark.catalog.tableExists(input_table):
        raise ValueError(f"[{label}] input table {input_table} does not exist")
    n_in = spark.table(input_table).count()
    if n_in == 0:
        raise ValueError(f"[{label}] input table {input_table} is empty")

    # Resolve the chart BEFORE the long poll, so a bad name fails in seconds.
    mclient.chart_reference(chart, "PPA", province)
    print(f"[{label}] scoring {input_table} ({n_in:,} rows) against {chart}")

    scored = onlevel_premiums(input_table, "PPA", province, uniq_keys, chart=chart)

    if scored is None:
        raise RuntimeError(
            f"""[{label}] pyRate scoring FAILED -- onlevel_premiums returned None.
  input  : {input_table}
  output : {out_table}  (never created)
  chart  : {chart}
The job status reached 'Failed' before the output table appeared. Scroll up to
the last '---- Iteration N ----' block for the pyRate error -- most often an
input column the proposed CHART needs that dataprep does not build.""")
    return scored

def score_both(table_base, uniq_keys, kind):
    """Copy the prepped table once per CHART and on-level each.
    Returns {"current": df, "proposed": df}."""
    out = {}
    for side, (chart, sfx) in SIDES.items():
        copy_table = f"{table_base}_{sfx}"
        if not (RESUME_SCORED and spark.catalog.tableExists(copy_table + "_onlvl")):
            spark.sql(f"drop table if exists {copy_table}")
            spark.sql(f"create table {copy_table} as select * from {table_base}")
        out[side] = onlevel(copy_table, uniq_keys, chart, f"{kind} {side}")
    return out

# ============================================================================
# CELL 12 - shared column maps and small helpers
# ============================================================================
premiums = ["Prm_Trm_BI_Uncap_Am",
            "Prm_Trm_PD_Uncap_Am",
            "Prm_Trm_DC_Uncap_Am",
            "Prm_Trm_AB_Uncap_Am",
            "Prm_Trm_AP_COL_Uncap_Am",
            "Prm_Trm_AP_SP_CMP_Uncap_Am",
            "Prm_Trm_UA_Uncap_Am",
            "Prm_Trm_UI_Uncap_Am"]

endorsements = {"Prm_Trm_End_02_Uncap_Am":  "End_02",
                "Prm_Trm_End_06_Uncap_Am":  "End_06A",
                "Prm_Trm_End_16_Uncap_Am":  "End_16",
                "Prm_Trm_End_20_Uncap_Am":  "End_20",
                "Prm_Trm_End_23B_Uncap_Am": "End_23B",
                "Prm_Trm_End_27_Uncap_Am":  "End_27",
                "Prm_Trm_End_27S_Uncap_Am": "End_27S",
                "Prm_Trm_End_38_Uncap_Am":  "End_38",
                "Prm_Trm_End_43_Uncap_Am":  "End_43",
                "Prm_Trm_End_43R_Uncap_Am": "End_43R",
                "Prm_Trm_End_43S_Uncap_Am": "End_43S",
                "Prm_Trm_End_60S_Uncap_Am": "End_60",   # kept as in the original
                "Prm_Trm_End_70_Uncap_Am":  "End_70"}

exposures = {"xpo_ern_chap_tpl_nb": "Xpo_Ern_TPL_Nb",
             "xpo_ern_chap_bi_nb":  "Xpo_Ern_BI_Nb",
             "xpo_ern_chap_pd_nb":  "Xpo_Ern_PD_Nb",
             "xpo_ern_chap_dc_nb":  "Xpo_Ern_DC_Nb",
             "xpo_ern_chap_ab_nb":  "Xpo_Ern_AB_Nb",
             "xpo_ern_chap_um_nb":  "Xpo_Ern_UA_Nb",
             "xpo_ern_chap_col_nb": "Xpo_Ern_COL_Nb",
             "xpo_ern_chap_cmp_nb": "Xpo_Ern_CMP_Nb",
             "xpo_ern_chap_ap_nb":  "Xpo_Ern_AP_Nb",
             "xpo_ern_chap_spe_nb": "Xpo_Ern_SPE_Nb",
             "xpo_ern_chap_ui_nb":  "Xpo_Ern_UI_Nb"}

COV_END = ["BI", "PD", "DC", "AB", "AP_COL", "AP_SP_CMP", "UA"]   # split-eligible
COV_ALL = COV_END + ["UI"]                                        # UI has no split

# (output coverage token, source coverage token) -- AP_SP_CMP is written out as
# AP_SP_COMP in the dislocation summaries. Order fixes output column order.
EARN_MAP = [("BI", "BI"), ("PD", "PD"), ("DC", "DC"), ("AB", "AB"), ("UA", "UA"),
            ("AP_COL", "AP_COL"), ("AP_SP_COMP", "AP_SP_CMP"), ("UI", "UI")]

CLAIM_COLS = ["clm_chap_bi_cap500k_am", "clm_chap_pd_am", "clm_chap_dc_am",
              "clm_chap_ab_cap500k_am", "clm_chap_um_am", "clm_chap_col_am",
              "clm_chap_cmp_am", "clm_chap_ui_am"]

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
    """Fail with the full list of what is missing, not one name at a time."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{where}: {len(missing)} column(s) missing: {missing}")

# ============================================================================
# CELL 13 - rate-change bucketing (replicates R cut(..., right = FALSE))
# ============================================================================
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

# ============================================================================
# CELL 14 - rating-variable banding (logic unchanged from the original)
# ============================================================================
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

# 5k bands from 10000 to 99999, then the four wide bands. Same 23 labels as the
# hand-written chain in the original.
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
    """The banding block the original repeats in both the TRX-driver and the
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
# CELL 15 - PART 1: TRX extract -> prep -> on-level (both CHARTs)
# ============================================================================
varlist  = (spark.read.options(delimiter=",", header=True)
                 .csv(helper_path + "ap_trx_data_extract_helper.csv").pandas_api())
endsplit = spark.read.options(delimiter=",", header=True).csv(helper_path + "endSplit.csv")
var_names = ",".join(varlist["var_name"].tolist())

print(f"TRX as of {as_of_date_suffix}, AY {dat_year_type}, province {province}")

df_trx_raw = spark.sql(f"""
select {var_names}
from prod_40tdds.tdds_ppod_actdata_auto_personal_trx_vw
    where pol_region_cd = "AP"
        and dat_as_of_dt = "{as_of_date}"
        and veh_product_cd in ("ppa")
        and dat_year_type_tx = "{dat_year_type}"
        and pol_commercial_in = 0
        -- Removing null transaction information: no claim and no exposure assigned
        and trx_offset_onset_cd is not null
""").filter(F.col("pol_jurisdiction_cd") == province)

df_trx_raw.groupBy("pol_jurisdiction_cd").count().show()

df_trx_prep = dataprep_trx(df_trx_raw)
test_merge_df(df_trx_raw, df_trx_prep)

table_prep_trx = f"{schema}.trx_{province.lower()}_ppa_prep_{as_of_date_suffix}"
spark.sql(f"drop table if exists {table_prep_trx}")
df_trx_prep.write.format("delta").mode("overwrite").saveAsTable(table_prep_trx)
print(f"TRX prep table: {table_prep_trx}")

uniq_keys_trx_score = ["clt_account_no", "pol_policy_no", "veh_id_no", "dri_id_no",
                       "pol_eff_dt", "trx_ent_dt", "trx_eff_dt", "trx_offset_onset_cd",
                       "pol_acc_yr_dt", "pol_face_next_ent_dt"]

trx_scored = score_both(table_prep_trx, uniq_keys_trx_score, "TRX")
for _side, _d in trx_scored.items():
    test_merge_df(df_trx_raw, _d)

# ============================================================================
# CELL 16 - endorsement split + earning, applied identically to both sides
# ============================================================================
def prepare_trx_side(d):
    """Rename / AP-fold / endorsement-split / earn. The original carries this
    block twice, once per side; any edit had to be made in both copies."""
    d = (d.withColumnsRenamed(endorsements)
          .withColumnsRenamed(exposures)
          .withColumn("Prm_Trm_COL_Uncap_Am",
                      F.when(F.col("cov_ap_in") == 1, 0).otherwise(F.col("Prm_Trm_COL_Uncap_Am")))
          .withColumn("Prm_Trm_CMP_Uncap_Am",
                      F.when(F.col("cov_ap_in") == 1, 0).otherwise(F.col("Prm_Trm_CMP_Uncap_Am")))
          .withColumn("Xpo_Ern_COL_Nb",
                      F.when(F.col("cov_ap_in") == 1, 0).otherwise(F.col("Xpo_Ern_COL_Nb")))
          .withColumn("Xpo_Ern_CMP_Nb",
                      F.when(F.col("cov_ap_in") == 1, 0).otherwise(F.col("Xpo_Ern_CMP_Nb")))
          .withColumn("Prm_Trm_AP_COL_Uncap_Am",
                      F.col("Coll_Portion_of_AP") + F.col("Prm_Trm_COL_Uncap_Am"))
          .withColumn("Prm_Trm_AP_SP_CMP_Uncap_Am",
                      F.col("Prm_Trm_SP_Uncap_Am") + F.col("Comp_Portion_of_AP")
                      + F.col("Prm_Trm_CMP_Uncap_Am"))
          .withColumn("Xpo_Ern_AP_COL_Nb",
                      F.col("Xpo_Ern_AP_Nb") + F.col("Xpo_Ern_COL_Nb"))
          .withColumn("Xpo_Ern_AP_SP_CMP_Nb",
                      F.col("Xpo_Ern_SPE_Nb") + F.col("Xpo_Ern_AP_Nb") + F.col("Xpo_Ern_CMP_Nb")))

    d = split_endorsements(d, list(endorsements.values()), premiums, endsplit)

    for cov in COV_END:
        d = d.withColumn(f"{cov}_Portion_All",
                         F.col(f"Prm_Trm_{cov}_Uncap_Am_End") - F.col(f"Prm_Trm_{cov}_Uncap_Am"))
    for cov in COV_ALL:
        d = d.withColumn(f"Prm_Ern_{cov}_OnLvl_Am",
                         F.col(f"Prm_Trm_{cov}_Uncap_Am") * F.col(f"Xpo_Ern_{cov}_Nb"))
    for cov in COV_ALL:
        src = f"Prm_Trm_{cov}_Uncap_Am" + ("" if cov == "UI" else "_End")
        d = d.withColumn(f"Prm_Ern_{cov}_OnLvl_End_Am",
                         F.col(src) * F.col(f"Xpo_Ern_{cov}_Nb"))
    return d

trx_final = {side: prepare_trx_side(d) for side, d in trx_scored.items()}
df_final_current, df_final_proposed = trx_final["current"], trx_final["proposed"]

# The two on-levelled TRX tables the original also writes.
for _side, _sfx in [("current", CUR), ("proposed", PRP)]:
    _t = f"{schema}.trx_{province.lower()}_ppa_prep_onlvl_{as_of_date_suffix}_{_sfx}"
    spark.sql(f"drop table if exists {_t}")
    trx_final[_side].write.format("delta").mode("overwrite").saveAsTable(_t)
    print(f"wrote {_t}")

# ============================================================================
# CELL 17 - PART 2: TRX dislocation, by vehicle and by driver
# ============================================================================
uniq_keys_trx = ["pol_insurer_no", "clt_account_no", "pol_policy_no", "veh_id_no",
                 "dri_id_no", "pol_eff_dt", "trx_eff_dt", "trx_ent_dt", "pol_acc_yr_dt",
                 "trx_offset_onset_cd", "pol_jurisdiction_cd", "vehicle_driver_version_key"]

# Proposed side: "_OnLvl_Am" -> "_OnLvl_Am_Prop" (leaves "_OnLvl_End_Am" alone).
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
    """Per-coverage earned premium plus the total, for one side. Replaces two
    hand-written 9-line blocks that appeared three times between them."""
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
    df_trx_veh_binned.select(*veh_group, "Rate_Change_Level"),
    on=veh_group, how="left")

df_trx_dri_prop = band_common(
    add_earned(add_earned(join_sides(df_trx_dri), "current"), "proposed"))

cols_to_keep_dri = ["Rate_Change_Level", "pol_acc_yr_dt", "pol_uwcompany_cd"] \
                   + c_variables + ["pol_business_cd"]

# (source coverage token, output coverage token) -- AP_COL/AP_SP_COMP are
# summarised as COL/CMP in the by-driver output.
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
# CELL 18 - PART 3: inforce extract -> prep -> CLEAR -> on-level (both CHARTs)
# ============================================================================
as_of_dt_f3 = inf_as_of_date[:-3].replace("-", "_")
print(f"Inforce as of {inf_as_of_date} ({as_of_dt_f3})")

df_inf_raw = spark.sql(f"""
            select *
            from prod_40tdds.tdds_ppod_actdata_inf_auto_personal_vw
                where pol_region_cd in ('AP')
                    and dat_as_of_dt in ('{inf_as_of_date}')
                    and veh_product_cd in ('ppa', 'motor_home')
                    and pol_commercial_in = 0
                    and pol_jurisdiction_cd in ('{province}')
""")
df_inf_raw.groupBy("pol_jurisdiction_cd").count().show()

df_inf_prep = dataprep_inf(df_inf_raw)
test_merge_df(df_inf_raw, df_inf_prep)

# GA2M loss cost (switched 20260108)
df_lc = spark.sql("""
                select pol_jurisdiction_cd, veh_product_cd, vehicle_driver_version_key,
                lc_losscost_bi_pred_rebase, lc_losscost_pd_pred_rebase, lc_losscost_dc_pred_rebase,
                lc_losscost_ab_pred_rebase, lc_losscost_coll_pred_rebase, lc_losscost_comp_pred_rebase,
                lc_losscost_total_pred_rebase
                from p_auto_lc.daily_lc_ga2m_ppa_policy_scored_master_t0
                where pol_region_cd in ('AP')
                    and veh_product_cd in ('ppa', 'motor_home')
""")

_LC_RENAMES = {"lc_losscost_bi_pred_rebase":    "lc_losscost_bi_pred_am",
               "lc_losscost_pd_pred_rebase":    "lc_losscost_pd_pred_am",
               "lc_losscost_dc_pred_rebase":    "lc_losscost_dc_pred_am",
               "lc_losscost_ab_pred_rebase":    "lc_losscost_ab_pred_am",
               "lc_losscost_coll_pred_rebase":  "lc_losscost_col_pred_am",
               "lc_losscost_comp_pred_rebase":  "lc_losscost_cmp_pred_am",
               "lc_losscost_total_pred_rebase": "lc_losscost_total_pred_am"}

require_columns(df_lc, list(_LC_RENAMES), "GA2M loss cost")
df_prep_lc = (df_inf_prep
              .join(df_lc, on=["vehicle_driver_version_key", "pol_jurisdiction_cd",
                               "veh_product_cd"], how="left")
              .withColumnsRenamed(_LC_RENAMES))

clear_tables = [t.name.rstrip("/")
                for t in dbutils.fs.ls(f"{abfss_root}/{username}/databaseSchema")
                if t.name.rstrip("/").startswith("ibc_clear_")]
clear_year = int(max(clear_tables)[-4:])
print(f"CLEAR {clear_year} will be used")

df_prep_clear = onlevel_clear_vrg(df_prep_lc, clear_year)
test_merge_df(df_inf_raw, df_prep_clear)

table_prep_inf = f"{schema}.inf_{province.lower()}_ppa_prep_{as_of_dt_f3}"
spark.sql(f"drop table if exists {table_prep_inf}")
df_prep_clear.write.format("delta").mode("overwrite").saveAsTable(table_prep_inf)

uniq_keys_inf = ["pol_policy_no", "veh_id_no", "dri_id_no", "pol_eff_dt"]
inf_scored = score_both(table_prep_inf, uniq_keys_inf, "INF")
for _side, _d in inf_scored.items():
    test_merge_df(df_inf_raw, _d)

# ============================================================================
# CELL 19 - PART 4: inforce dislocation by vehicle
# ============================================================================
inf_cur, inf_prop = inf_scored["current"], inf_scored["proposed"]

for _c in inf_prop.columns:
    if _c.endswith("Uncap_Am"):
        inf_prop = inf_prop.withColumnRenamed(_c, _c.replace("Uncap_Am", "Uncap_Am_Prop"))

prop_keep = list(dict.fromkeys(
    [c for c in inf_prop.columns if c.startswith("Prm_Trm_")]
    + uniq_keys_inf + ["pol_uwcompany_cd"]))
inf_join = inf_cur.join(inf_prop.select(*prop_keep),
                        on=uniq_keys_inf + ["pol_uwcompany_cd"], how="left")

veh_keys = ["pol_uwcompany_cd", "pol_insurer_no", "clt_account_no",
            "pol_policy_no", "veh_id_no"]
w_veh = Window.partitionBy(*veh_keys)

# AP_SP_CMP vs AP_SP_COMP: the CHART decides which spelling lands in the scored
# table, so take whichever exists.
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
# CELL 20 - export. ADIDO 2569, parquet, the same three files as the original.
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
