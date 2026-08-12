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
dbutils.widgets.text("as_of_date_suffix", "202603")

province = dbutils.widgets.get("province")
as_of_date_suffix = dbutils.widgets.get("as_of_date_suffix")

FSA_COL = "veh_fsa_tx"   # or "pol_fsa_tx"
VARS = ["fsa_tx"]

premium_cols = [
    "Prm_Ern_BI_OnLvl_End_Am",
    "Prm_Ern_PD_OnLvl_End_Am",
    "Prm_Ern_DC_OnLvl_End_Am",
    "Prm_Ern_AB_OnLvl_End_Am",
    "Prm_Ern_AP_COL_OnLvl_End_Am",
    "Prm_Ern_AP_SP_CMP_OnLvl_End_Am"
]

expo_cols = [
    "Xpo_Ern_BI_Nb",
    "Xpo_Ern_PD_Nb",
    "Xpo_Ern_DC_Nb",
    "Xpo_Ern_AB_Nb",
    "Xpo_Ern_AP_COL_Nb",
    "Xpo_Ern_AP_SP_CMP_Nb"
]

clm_nb_cols = [
    "clm_chap_tpl_nb",
    "clm_chap_bi_nb",
    "clm_chap_dc_nb",
    "clm_chap_pd_nb",
    "clm_chap_ab_nb",
    "clm_chap_col_nb",
    "clm_chap_cmp_nb"
]

clm_am_cols = [
    "clm_chap_tpl_cap500k_am",
    "clm_chap_bi_cap500k_am",
    "clm_chap_pd_cap500k_am",
    "clm_chap_dc_am",
    "clm_chap_ab_cap500k_am",
    "clm_chap_col_am",
    "clm_chap_cmp_am"
]

base_group_cols = ["pol_jurisdiction_cd", "pol_uwcompany_cd", "pol_acc_yr_dt"]
measure_cols = premium_cols + expo_cols + clm_nb_cols + clm_am_cols
required_cols = base_group_cols + measure_cols + ["dri_type_cd", FSA_COL]

# Prm_Ern_* / Xpo_Ern_* are built after the four provinces are unioned back together,
# so they only exist on the AP-wide table. Try each name, take the first that has them.
df_trx = None
tried = []
for t in [f"t_ap_ppa_pricing.trx_{province.lower()}_ppa_prep_onlvl_{as_of_date_suffix}",
          f"t_ap_ppa_pricing.trx_{province.lower()}_ppa_prep_{as_of_date_suffix}_onlvl",
          f"t_ap_ppa_pricing.trx_ap_ppa_prep_onlvl_{as_of_date_suffix}"]:
    try:
        d = spark.table(t)
    except AnalysisException:
        tried.append(f"{t} -> missing table")
        continue
    gaps = [c for c in required_cols if c not in d.columns]
    if gaps:
        tried.append(f"{t} -> missing {gaps[:5]}")
        continue
    df_trx, trx_table = d, t
    break
if df_trx is None:
    raise ValueError("no usable TRX table:\n  " + "\n  ".join(tried))
print(f"using {trx_table}")

df_trx = df_trx.filter(F.col("pol_jurisdiction_cd") == F.lit(province.upper()))

not_null_all_premiums = reduce(
    lambda acc, c: acc & F.col(c).isNotNull(),
    premium_cols,
    F.lit(True)
)

df_trx_prep = (df_trx
    .filter(
        (F.col("dri_type_cd") != F.lit("OccasionalPrincipal")) &
        not_null_all_premiums
    )
    .withColumn(
        "pol_uwcompany_cd",
        F.when(F.col("pol_uwcompany_cd") == F.lit("snic"), F.lit("SN"))
         .when(F.col("pol_uwcompany_cd") == F.lit("prim"), F.lit("PIC"))
         .otherwise(F.lit("TDHA"))
    )
)

df_trx_prep = ClassificationPrep(df_trx_prep)

# Raw FSA: upper/trim, nulls to UNKNOWN so the group-by does not drop those rows.
_fsa = F.upper(F.trim(F.col(FSA_COL)))
df_trx_prep = df_trx_prep.withColumn(
    "fsa_tx",
    F.when(_fsa.isNull() | (_fsa == F.lit("")), F.lit("UNKNOWN")).otherwise(_fsa)
)

_chk = df_trx_prep.agg(
    F.count(F.lit(1)).alias("rows"),
    F.countDistinct(F.col("fsa_tx")).alias("cells"),
    F.sum(F.when(F.col("fsa_tx") == "UNKNOWN", 1).otherwise(0)).alias("unknown")
).collect()[0]
print(f"rows={_chk['rows']:,}  FSA cells={_chk['cells']:,}  UNKNOWN={_chk['unknown']:,}")

for v in VARS:
    group_cols = base_group_cols + [v]

    df_sel = df_trx_prep.select(*(group_cols + measure_cols))

    agg_exprs = [F.sum(F.col(c)).alias(c) for c in measure_cols]

    df_out = (df_sel
        .groupBy(*group_cols)
        .agg(*agg_exprs)
    )

    adido_out(table = df_out, ticket = 3868, filename = 'Trx_on_ppa_Ay_onlvl_wRels', freeForm = f"TRX_{v}", fileformat='parquet', folder_out = f't_ap_ppa_pricing/data/{province.lower()}/classification/')

df_out.display()
