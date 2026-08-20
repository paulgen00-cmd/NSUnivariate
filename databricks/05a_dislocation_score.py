# ============================================================================
# 05a_dislocation_score.py -- score ONE dataset against ONE chart.
#
# Run this FOUR times, changing two widgets each time:
#
#     dataset=trx  RUO_Chart=<current>      ->  trx_<prov>_ppa_prep_onlvl_<suffix>_<CUR>
#     dataset=trx  RUO_Chart=<proposed>     ->  trx_<prov>_ppa_prep_onlvl_<suffix>_<PRP>
#     dataset=inf  RUO_Chart=<current>      ->  inf_<prov>_ppa_prep_<f3>_<CUR>_onlvl
#     dataset=inf  RUO_Chart=<proposed>     ->  inf_<prov>_ppa_prep_<f3>_<PRP>_onlvl
#
# Then run 05b_dislocation_summary.py once. It reads those four tables and
# writes the three ADIDO files.
#
# WHY SPLIT: the original notebook calls onlevel_premiums twice back to back in
# one session and the second call fails within ~60s while the first succeeds.
# One scoring per execution is the path that is known to work. It also makes
# every step resumable -- a failure costs one scoring, not all four.
#
# THIS NOTEBOOK MAKES EXACTLY ONE onlevel_premiums CALL. Do not add a second.
#
# ############################################################################
# HOW TO PASTE THIS IN -- read this or the first cell silently does nothing.
#
# A Databricks magic only works as the FIRST LINE of its cell. Paste ONLY the
# `%run ...` line into each %run cell -- NOT the `# CELL n` banner above it.
# A banner above the magic makes Databricks treat the cell as ordinary Python,
# the %run is skipped, and the functions it should load are never defined. The
# symptom is a NameError several cells later, e.g. "name 'dataprep' is not
# defined".
#
# CELL 9 preflights every name the %run cells should have loaded and names the
# ones that are missing, so run it before starting an 8-minute scoring.
# ############################################################################
# ============================================================================

# --- CELL 1: paste ONLY the next line, nothing above it ---------------------

%run /Workspace/Shared/t_ap_ppa_pricing/Functions/Utils

# --- CELL 2: paste ONLY the next line, nothing above it ---------------------

%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_Onlevel

# --- CELL 3: paste ONLY the next line, nothing above it ---------------------

%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_Endorsement_Split

# --- CELL 4: paste ONLY the next line, nothing above it ---------------------

%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_DataPrep

# ============================================================================
# CELL 5 - capture the TRX dataprep before the inforce %run rebinds the name
# ============================================================================
try:
    dataprep_trx = dataprep
except NameError:
    raise NameError("""`dataprep` is not defined, so CELL 4 did not take.
A Databricks %run only works as the FIRST LINE of its cell. If you pasted the
'# --- CELL 4' comment above it, the magic was skipped.
Fix: put ONLY this line in that cell, with nothing above it:
    %run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_DataPrep
then re-run CELL 4 and CELL 5.""")

# --- CELL 6: paste ONLY the next line, nothing above it ---------------------

%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_DataPrep_Inforce

# ============================================================================
# CELL 7 - capture the inforce dataprep
# ============================================================================
try:
    dataprep_inf = dataprep
except NameError:
    raise NameError("""`dataprep` is not defined, so CELL 6 did not take.
Put ONLY this line in that cell, with nothing above it:
    %run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_DataPrep_Inforce
then re-run CELL 6 and CELL 7.""")

# `dataprep` now refers to the inforce version. dataprep_trx / dataprep_inf are
# the two captured names -- use those from here on, never bare `dataprep`.

# --- CELL 8: paste ONLY the next line, nothing above it ---------------------

%run /Workspace/Shared/t_ap_ppa_pricing/Functions/AP_PPA_CLEAR_Onlevel

# ============================================================================
# CELL 9 - everything below is one cell
# ============================================================================
import pyspark.sql.functions as F
from ARRR import ARRR

# --- preflight: did every %run cell actually take? -------------------------
_NEEDS = [("test_merge_df",      "CELL 1  Utils"),
          ("adido_out",          "CELL 1  Utils"),
          ("onlevel_premiums",   "CELL 2  AP_PPA_Onlevel"),
          ("split_endorsements", "CELL 3  AP_PPA_Endorsement_Split"),
          ("dataprep_trx",       "CELL 4+5  AP_PPA_DataPrep"),
          ("dataprep_inf",       "CELL 6+7  AP_PPA_DataPrep_Inforce"),
          ("onlevel_clear_vrg",  "CELL 8  AP_PPA_CLEAR_Onlevel")]
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
    "dataset":               "trx",                      # trx | inf
    "RUO_Chart":             "NS.PPA.20260702.json",
    "province":              "NS",
    "trx_as_of_date":        "2024-10-25",
    "trx_as_of_date_suffix": "202410",
    "trx_dat_year_type":     "nov-oct",
    "inf_as_of_date":        "2026-03-27",
    "schema":                "u_wuhanz5",
}
for _n, _d in _DEFAULTS.items():
    dbutils.widgets.text(_n, _d)

def W(name):
    v = dbutils.widgets.get(name).strip()
    return v if v else _DEFAULTS[name]

dataset           = W("dataset").lower()
province          = W("province").upper()
as_of_date        = W("trx_as_of_date")
as_of_date_suffix = W("trx_as_of_date_suffix")
dat_year_type     = W("trx_dat_year_type")
inf_as_of_date    = W("inf_as_of_date")
schema            = W("schema")

if dataset not in ("trx", "inf"):
    raise ValueError(f"dataset must be 'trx' or 'inf', got {dataset!r}")

username    = "t_ap_ppa_pricing"
abfss_root  = "abfss://home@edaaaazepca0tdi0cpa0001.dfs.core.windows.net"
helper_path = f"{abfss_root}/{username}/data/mapping/"

# --- chart resolution ------------------------------------------------------
mclient = ARRR()
mclient.update_files()

def chart_suffix(chart_name):
    """Table-safe suffix. Strips '.json' only when it is actually there --
    a bare chart[:-5] chops five real characters off a name entered without
    the extension, producing a truncated table name and an unresolvable chart."""
    base = chart_name[:-5] if chart_name.lower().endswith(".json") else chart_name
    return base.replace(".", "_")

RUO_chart = W("RUO_Chart").strip()
if not RUO_chart.lower().endswith(".json"):
    RUO_chart += ".json"

_available = mclient.available_charts(product="PPA", region=province)
if RUO_chart not in _available:
    raise ValueError(f"chart {RUO_chart!r} is not published for PPA/{province}.\n"
                     f"Available: {_available}")
CHART = chart_suffix(RUO_chart)
mclient.chart_reference(RUO_chart, "PPA", province)   # fails now, not after the poll
print(f"chart {RUO_chart} -> table suffix {CHART}")

# --- the one guarded scoring call ------------------------------------------
def onlevel_once(input_table, uniq_keys):
    """onlevel_premiums with its silent-None failure mode closed off.

    The shared function polls until {input_table}_onlvl appears and its only
    exit WITH a DataFrame is the break; when the pyRate job reaches 'Failed'
    first, the while condition ends the loop and it returns None with no
    exception. That None surfaces later as
    "'NoneType' object has no attribute 'count'" inside test_merge_df.
    """
    out_table = input_table + "_onlvl"
    if not spark.catalog.tableExists(input_table):
        raise ValueError(f"input table {input_table} does not exist")
    n_in = spark.table(input_table).count()
    if n_in == 0:
        raise ValueError(f"input table {input_table} is empty")

    print(f"scoring {input_table} ({n_in:,} rows) against {RUO_chart}")
    scored = onlevel_premiums(input_table, "PPA", province, uniq_keys, chart=RUO_chart)

    if scored is None:
        raise RuntimeError(
            f"""pyRate scoring FAILED -- onlevel_premiums returned None.
  input  : {input_table}
  output : {out_table}  (never created)
  chart  : {RUO_chart}
The job status reached 'Failed' before the output table appeared. Scroll up to
the last '---- Iteration N ----' block for the pyRate error.""")
    return scored

# ============================================================================
# CELL 10 - TRX branch. Skipped entirely when dataset='inf'.
# ============================================================================
if dataset == "trx":
    varlist = (spark.read.options(delimiter=",", header=True)
                    .csv(helper_path + "ap_trx_data_extract_helper.csv").pandas_api())
    endsplit = spark.read.options(delimiter=",", header=True).csv(helper_path + "endSplit.csv")
    var_names = ",".join(varlist["var_name"].tolist())

    df_raw = spark.sql(f"""
    select {var_names}
    from prod_40tdds.tdds_ppod_actdata_auto_personal_trx_vw
        where pol_region_cd = "AP"
            and dat_as_of_dt = "{as_of_date}"
            and veh_product_cd in ("ppa")
            and dat_year_type_tx = "{dat_year_type}"
            and pol_commercial_in = 0
            -- null transaction info has no claim and no exposure assigned
            and trx_offset_onset_cd is not null
    """).filter(F.col("pol_jurisdiction_cd") == province)

    n_raw = df_raw.count()
    # Compare this number between the current and the proposed run. If it moves,
    # the two sides were built on different source data and must not be joined.
    print(f"*** TRX RAW ROW COUNT = {n_raw:,}  (must match across both chart runs)")

    df_prep = dataprep_trx(df_raw)
    test_merge_df(df_raw, df_prep)

    table_prep = f"{schema}.trx_{province.lower()}_ppa_prep_{as_of_date_suffix}"
    spark.sql(f"drop table if exists {table_prep}")
    df_prep.write.format("delta").mode("overwrite").saveAsTable(table_prep)

    # One copy per chart, so the two scorings never share an input table.
    copy_table = f"{table_prep}_{CHART}"
    spark.sql(f"drop table if exists {copy_table}")
    spark.sql(f"create table {copy_table} as select * from {table_prep}")

    uniq_keys = ["clt_account_no", "pol_policy_no", "veh_id_no", "dri_id_no",
                 "pol_eff_dt", "trx_ent_dt", "trx_eff_dt", "trx_offset_onset_cd",
                 "pol_acc_yr_dt", "pol_face_next_ent_dt"]

    df_scored = onlevel_once(copy_table, uniq_keys)
    test_merge_df(df_raw, df_scored)

# ============================================================================
# CELL 11 - TRX endorsement split + earning, then persist. dataset='trx' only.
# ============================================================================
if dataset == "trx":
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
                    "Prm_Trm_End_60S_Uncap_Am": "End_60",   # as in the original
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

    d = (df_scored
         .withColumnsRenamed(endorsements)
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

    OUT = f"{schema}.trx_{province.lower()}_ppa_prep_onlvl_{as_of_date_suffix}_{CHART}"
    spark.sql(f"drop table if exists {OUT}")
    d.write.format("delta").mode("overwrite").saveAsTable(OUT)
    print(f"\nDONE. 05b will read: {OUT}")

# ============================================================================
# CELL 12 - inforce branch. Skipped entirely when dataset='trx'.
# ============================================================================
if dataset == "inf":
    as_of_dt_f3 = inf_as_of_date[:-3].replace("-", "_")

    df_raw = spark.sql(f"""
                select *
                from prod_40tdds.tdds_ppod_actdata_inf_auto_personal_vw
                    where pol_region_cd in ('AP')
                        and dat_as_of_dt in ('{inf_as_of_date}')
                        and veh_product_cd in ('ppa', 'motor_home')
                        and pol_commercial_in = 0
                        and pol_jurisdiction_cd in ('{province}')
    """)
    n_raw = df_raw.count()
    print(f"*** INF RAW ROW COUNT = {n_raw:,}  (must match across both chart runs)")

    df_prep = dataprep_inf(df_raw)
    test_merge_df(df_raw, df_prep)

    # GA2M loss cost (switched 20260108)
    df_lc = spark.sql("""
        select pol_jurisdiction_cd, veh_product_cd, vehicle_driver_version_key,
               lc_losscost_bi_pred_rebase, lc_losscost_pd_pred_rebase,
               lc_losscost_dc_pred_rebase, lc_losscost_ab_pred_rebase,
               lc_losscost_coll_pred_rebase, lc_losscost_comp_pred_rebase,
               lc_losscost_total_pred_rebase
        from p_auto_lc.daily_lc_ga2m_ppa_policy_scored_master_t0
        where pol_region_cd in ('AP') and veh_product_cd in ('ppa', 'motor_home')
    """)

    _LC = {"lc_losscost_bi_pred_rebase":    "lc_losscost_bi_pred_am",
           "lc_losscost_pd_pred_rebase":    "lc_losscost_pd_pred_am",
           "lc_losscost_dc_pred_rebase":    "lc_losscost_dc_pred_am",
           "lc_losscost_ab_pred_rebase":    "lc_losscost_ab_pred_am",
           "lc_losscost_coll_pred_rebase":  "lc_losscost_col_pred_am",
           "lc_losscost_comp_pred_rebase":  "lc_losscost_cmp_pred_am",
           "lc_losscost_total_pred_rebase": "lc_losscost_total_pred_am"}

    _missing_lc = [c for c in _LC if c not in df_lc.columns]
    if _missing_lc:
        raise ValueError(f"GA2M loss cost table is missing: {_missing_lc}")

    df_prep_lc = (df_prep
                  .join(df_lc, on=["vehicle_driver_version_key", "pol_jurisdiction_cd",
                                   "veh_product_cd"], how="left")
                  .withColumnsRenamed(_LC))

    clear_tables = [t.name.rstrip("/")
                    for t in dbutils.fs.ls(f"{abfss_root}/{username}/databaseSchema")
                    if t.name.rstrip("/").startswith("ibc_clear_")]
    clear_year = int(max(clear_tables)[-4:])
    print(f"CLEAR {clear_year} will be used")

    df_prep_clear = onlevel_clear_vrg(df_prep_lc, clear_year)
    test_merge_df(df_raw, df_prep_clear)

    table_inf = f"{schema}.inf_{province.lower()}_ppa_prep_{as_of_dt_f3}_{CHART}"
    spark.sql(f"drop table if exists {table_inf}")
    df_prep_clear.write.format("delta").mode("overwrite").saveAsTable(table_inf)

    uniq_keys = ["pol_policy_no", "veh_id_no", "dri_id_no", "pol_eff_dt"]
    df_scored = onlevel_once(table_inf, uniq_keys)
    test_merge_df(df_raw, df_scored)

    print(f"\nDONE. 05b will read: {table_inf}_onlvl")
