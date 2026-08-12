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

premium_cols = [
    "Prm_Trm_BI_Uncap_Am",
    "Prm_Trm_DC_Uncap_Am",
    "Prm_Trm_PD_Uncap_Am",
    "Prm_Trm_AB_Uncap_Am",
    "Prm_Trm_AP_COL_Uncap_Am",
    "Prm_Trm_AP_SP_CMP_Uncap_Am"
]

raw_cov_cols = ["cov_bi_in", "cov_dc_in", "cov_pd_in", "cov_ab_in",
                "cov_ap_in", "cov_col_in", "cov_spe_in", "cov_cmp_in"]

cov_cols = ["cov_bi_in", "cov_dc_in", "cov_pd_in", "cov_ab_in",
            "cov_ap_col_in", "cov_ap_sp_cmp_in"]

required_cols = premium_cols + raw_cov_cols + ["dri_type_cd", "pol_uwcompany_cd",
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

# No jurisdiction in the group-by, so an AP-wide table would blend four provinces.
df_inf = df_inf.filter(F.col("pol_jurisdiction_cd") == F.lit(province.upper()))

not_null_all_premiums = reduce(
    lambda acc, c: acc & F.col(c).isNotNull(),
    premium_cols,
    F.lit(True)
)

# coalesce to 0: a null operand would null the whole sum and drop a real exposure.
df_inf = (df_inf
    .withColumn("cov_ap_col_in",
                F.coalesce(F.col("cov_ap_in"), F.lit(0)) + F.coalesce(F.col("cov_col_in"), F.lit(0)))
    .withColumn("cov_ap_sp_cmp_in",
                F.coalesce(F.col("cov_ap_in"), F.lit(0))
                + F.coalesce(F.col("cov_spe_in"), F.lit(0))
                + F.coalesce(F.col("cov_cmp_in"), F.lit(0)))
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

# AP/COL/CMP/SPE are mutually exclusive after dataprep; >1 means exposure double counted.
_bad = df_inf_prep.filter((F.col("cov_ap_col_in") > 1) | (F.col("cov_ap_sp_cmp_in") > 1)).count()
assert _bad == 0, f"{_bad:,} rows double count AP exposure - did this table go through dataprep()?"

for v in VARS:
    group_cols = ["pol_uwcompany_cd", v]

    df_sel = df_inf_prep.select(*(group_cols + premium_cols + cov_cols))

    df_out = (df_sel
        .groupBy(*group_cols)
        .agg(
            F.sum("cov_bi_in").alias("Xpo_BI"),
            F.sum("cov_dc_in").alias("Xpo_DC"),
            F.sum("cov_pd_in").alias("Xpo_PD"),
            F.sum("cov_ab_in").alias("Xpo_AB"),
            F.sum("cov_ap_col_in").alias("Xpo_AP_COL"),
            F.sum("cov_ap_sp_cmp_in").alias("Xpo_AP_SP_CMP"),

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
