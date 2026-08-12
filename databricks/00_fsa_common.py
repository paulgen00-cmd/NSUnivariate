%md
# FSA grouping variable + preflight guards — shared definition

`%run` this from each of the three classification notebooks so all three exhibits group on
exactly the same key and fail the same way when an input is wrong.

- `veh_fsa_tx` is the **garaging FSA of the vehicle** — the geography the IBC territory
  (`ter_onlvl_ibc_no`) is itself derived from, so it is the like-for-like swap.
- `pol_fsa_tx` (policy mailing FSA) also exists on both source views. Change
  `FSA_SOURCE_COL` below if that is the one you want; do not mix the two across exhibits.
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

# --- config -----------------------------------------------------------------
FSA_SOURCE_COL = "veh_fsa_tx"   # or "pol_fsa_tx"
FSA_VAR        = "fsa_tx"       # name of the grouping column in the output

# When True, FSAs that do not match the Canadian pattern (letter-digit-letter) are
# collapsed to "INVALID" instead of being carried through as-is. Default False so the
# exhibit shows the raw values exactly as they land.
FSA_STRICT = False

FSA_PATTERN = r"^[A-Z][0-9][A-Z]$"


# ---------------------------------------------------------------------------
# Preflight guards
# ---------------------------------------------------------------------------
def require_columns(df: DataFrame, cols, context: str) -> None:
    """Fail immediately, with the full list of what is missing, instead of dying on the
    first bad reference deep inside a select or an agg.

    Spark's own error names one column at a time, so a table that is missing six columns
    costs six round trips to diagnose. Worse, `withColumnRenamed` on a column that does not
    exist is a SILENT NO-OP — the ELR notebook renames ten columns and would carry on
    happily with none of them renamed, only to fail later with a confusing message about a
    name you never typed. This turns all of that into one readable failure.
    """
    have = set(df.columns)
    missing = [c for c in cols if c not in have]
    if missing:
        raise ValueError(
            f"{context}: {len(missing)} required column(s) missing: {missing}\n"
            f"Table has {len(df.columns)} columns. Check that you are pointed at the "
            f"on-leveled table and not the prepped one."
        )


def table_exists(name: str) -> bool:
    try:
        spark.sql(f"describe table {name}")
        return True
    except AnalysisException:
        return False


def resolve_table(candidates, required_cols, context: str):
    """Return (df, name) for the first candidate table that exists AND carries every
    required column.

    The TRX classification code in issue #1 reads
    `trx_{prov}_ppa_prep_onlvl_{suffix}`, which matches no table the pipeline writes
    (see README, "The TRX source table"). Rather than hard-code a replacement and risk
    breaking a table this repo cannot see, this tries each known naming pattern in order
    and reports exactly what it found and rejected.
    """
    tried = []
    for name in candidates:
        if not table_exists(name):
            tried.append(f"  {name} -> does not exist")
            continue
        df = spark.table(name)
        missing = [c for c in required_cols if c not in set(df.columns)]
        if missing:
            tried.append(f"  {name} -> exists but missing {len(missing)}: {missing[:6]}")
            continue
        print(f"[{context}] using {name}")
        return df, name

    raise ValueError(
        f"{context}: no candidate table is usable.\n" + "\n".join(tried)
    )


# ---------------------------------------------------------------------------
# FSA derivation
# ---------------------------------------------------------------------------
def add_fsa(df: DataFrame) -> DataFrame:
    """Derive the FSA grouping column.

    `dataprep()` already upper-cases `veh_fsa_tx`, but this repeats the normalization so
    the grouping key can never split on case or stray whitespace. Nulls and blanks land in
    a single "UNKNOWN" bucket rather than being dropped by the group-by — an FSA-level
    exhibit that silently loses rows will not tie back to the territory version.
    """
    if FSA_SOURCE_COL not in df.columns:
        raise ValueError(
            f"{FSA_SOURCE_COL} is not on this table. The inforce pipeline selects *, but the "
            f"TRX extract selects only the columns in ap_trx_data_extract_helper.csv — add "
            f"{FSA_SOURCE_COL} there and re-run 06_ap_ppa_trx_pipeline.py."
        )

    fsa = F.upper(F.trim(F.col(FSA_SOURCE_COL)))
    fsa = F.when(fsa.isNull() | (fsa == F.lit("")), F.lit("UNKNOWN")).otherwise(fsa)

    if FSA_STRICT:
        fsa = F.when(fsa == F.lit("UNKNOWN"), fsa) \
               .when(fsa.rlike(FSA_PATTERN), fsa) \
               .otherwise(F.lit("INVALID"))

    return df.withColumn(FSA_VAR, fsa)


def fsa_coverage_check(df: DataFrame, label: str) -> None:
    """Report cell count and the UNKNOWN/INVALID share in ONE pass over the data.

    An FSA cell count in the dozens-to-low-hundreds is expected (NS has roughly 80 live
    FSAs); a large UNKNOWN share is not, and means the source column is not populated the
    way it is on the territory path.
    """
    bad = F.col(FSA_VAR).isin("UNKNOWN", "INVALID")
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(F.col(FSA_VAR)).alias("cells"),
        F.sum(F.when(bad, F.lit(1)).otherwise(F.lit(0))).alias("bad"),
    ).collect()[0]

    rows, cells, n_bad = row["rows"], row["cells"], row["bad"] or 0
    pct = (n_bad / rows * 100) if rows else 0.0
    print(f"[{label}] rows={rows:,}  distinct FSA={cells:,}  UNKNOWN/INVALID={n_bad:,} ({pct:.2f}%)")
    if pct > 1.0:
        print(f"[{label}] WARNING: more than 1% of rows have no usable FSA. Do not publish "
              f"this exhibit until you know why.")


def thin_cell_report(df_out: DataFrame, exposure_col: str, label: str, floor: int = 100) -> None:
    """Count output cells below an exposure floor.

    These extracts deliberately do no suppression or credibility grouping — that is a
    downstream decision. But FSA cells are an order of magnitude thinner than territory
    cells, so print how much of the output is too thin to read as signal.
    """
    if exposure_col not in df_out.columns:
        print(f"[{label}] thin-cell report skipped: {exposure_col} not in output")
        return
    total = df_out.count()
    thin = df_out.filter(F.col(exposure_col) < F.lit(floor)).count()
    print(f"[{label}] {thin:,} of {total:,} output cells have {exposure_col} < {floor} "
          f"({(thin / total * 100 if total else 0):.1f}%)")
