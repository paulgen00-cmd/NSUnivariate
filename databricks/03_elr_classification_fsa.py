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
# ============================================================================
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from functools import reduce

dbutils.widgets.text("province", "NS")
dbutils.widgets.text("as_of_date", "2026_03")

province = dbutils.widgets.get("province")
as_of_date = dbutils.widgets.get("as_of_date")

FSA_COL = "veh_fsa_tx"   # or "pol_fsa_tx"
VARS = ["fsa_tx"]

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

# (suffix, premium after rename, loss cost) - keeps the LC zeroing and the LRs in step.
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

required_cols = list(rename_map) + lc_source_cols + ["dri_type_cd", "pol_uwcompany_cd",
                                                     "pol_jurisdiction_cd", FSA_COL]

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
print(f"using {inf_table}")

df_inf = df_inf.filter(F.col("pol_jurisdiction_cd") == F.lit(province.upper()))

for old, new in rename_map.items():
    df_inf = df_inf.withColumnRenamed(old, new)

for name, prm, lc in COVERAGES:
    df_inf = df_inf.withColumn(
        f"LC_{name}",
        F.when(F.col(prm).isNull(), F.lit(0.0)).otherwise(F.col(lc))
    )

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

_fsa = F.upper(F.trim(F.col(FSA_COL)))
df_inf_prep = df_inf_prep.withColumn(
    "fsa_tx",
    F.when(_fsa.isNull() | (_fsa == F.lit("")), F.lit("UNKNOWN")).otherwise(_fsa)
)

_chk = df_inf_prep.agg(
    F.count(F.lit(1)).alias("rows"),
    F.countDistinct(F.col("fsa_tx")).alias("cells"),
    F.sum(F.when(F.col("fsa_tx") == "UNKNOWN", 1).otherwise(0)).alias("unknown")
).collect()[0]
print(f"rows={_chk['rows']:,}  FSA cells={_chk['cells']:,}  UNKNOWN={_chk['unknown']:,}")

for v in VARS:
    group_cols = ["pol_uwcompany_cd", v]
    select_cols = group_cols + premium_cols + lc_cols

    df_sel = df_inf_prep.select(*select_cols)

    agg_exprs = [F.sum(F.col(c)).alias(c) for c in (premium_cols + lc_cols)]

    df_out = (df_sel
        .groupBy(*group_cols)
        .agg(*agg_exprs)
    )

    # null premium treated like zero, so it matches the zeroed LC above
    for name, prm, _ in COVERAGES:
        df_out = df_out.withColumn(
            f"LR_{name}",
            F.when(F.col(prm).isNull() | (F.col(prm) == 0), F.lit(0.0))
             .otherwise(F.col(f"LC_{name}") / F.col(prm))
        )

    adido_out(table = df_out, ticket = 2569, filename = 'inf_ap_ppa_prep_onlvl', freeForm = f"ELR_Inf_{v}", fileformat='parquet', folder_out = f't_ap_ppa_pricing/data/{province.lower()}/classification/')

df_out.display()
