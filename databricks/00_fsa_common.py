%md
# FSA grouping variable — shared definition

`%run` this from each of the three classification notebooks so all three exhibits group on
exactly the same key. This is the ONLY thing that differs from the `ter_onlvl_ibc_no` versions.

- `veh_fsa_tx` is the **garaging FSA of the vehicle** — the geography the IBC territory
  (`ter_onlvl_ibc_no`) is itself derived from, so it is the like-for-like swap.
- `pol_fsa_tx` (policy mailing FSA) also exists on both source views. Change
  `FSA_SOURCE_COL` below if that is the one you want; do not mix the two across exhibits.
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# --- config -----------------------------------------------------------------
FSA_SOURCE_COL = "veh_fsa_tx"   # or "pol_fsa_tx"
FSA_VAR        = "fsa_tx"       # name of the grouping column in the output

# When True, FSAs that do not match the Canadian pattern (letter-digit-letter) are
# collapsed to "INVALID" instead of being carried through as-is. Default False so the
# exhibit shows the raw values exactly as they land.
FSA_STRICT = False

FSA_PATTERN = r"^[A-Z][0-9][A-Z]$"


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
    """Print how much of the book lands in UNKNOWN/INVALID before the aggregation runs.

    An FSA cell count in the hundreds is expected (NS alone has ~80 live FSAs); a large
    UNKNOWN share is not, and means the source column is not populated the way it is on
    the territory path.
    """
    total = df.count()
    buckets = df.groupBy(FSA_VAR).count()
    n_cells = buckets.count()
    bad = buckets.filter(F.col(FSA_VAR).isin("UNKNOWN", "INVALID")) \
                 .agg(F.coalesce(F.sum("count"), F.lit(0)).alias("n")).collect()[0]["n"]
    print(f"[{label}] rows={total:,}  distinct FSA={n_cells:,}  UNKNOWN/INVALID={bad:,} "
          f"({(bad / total * 100 if total else 0):.2f}%)")
