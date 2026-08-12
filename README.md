# NSUnivariate

FSA-level versions of the three Atlantic PPA classification extracts. Same outputs as the
production `ter_onlvl_ibc_no` runs — same filters, same measures, same ADIDO tickets — with
the grouping variable swapped from the IBC territory to the **raw FSA**.

Source of the originals: issue #1 of this repo (body = TRX, comments = ELR and Inforce).

## Notebooks (`databricks/`)

| File | Replaces | Grain | Output |
|---|---|---|---|
| `00_fsa_common.py` | — | — | `FSA_VAR` definition + `add_fsa()` / `fsa_coverage_check()`, `%run` by the other three |
| `01_trx_classification_fsa.py` | TRX extract | jurisdiction x company x accident year x FSA | ADIDO 3868, `TRX_fsa_tx` |
| `02_inforce_classification_fsa.py` | Inforce extract | company x FSA | ADIDO 2569, `Inforce_fsa_tx` |
| `03_elr_classification_fsa.py` | ELR extract | company x FSA | ADIDO 2569, `ELR_Inf_fsa_tx` |

Widgets are unchanged: `province` + `as_of_date_suffix` (TRX) or `as_of_date` (inforce/ELR).

## What changed vs. the originals

Everything outside these four points is byte-identical to the issue #1 code.

1. `VARS = ["ter_onlvl_ibc_no"]` -> `VARS = [FSA_VAR]`.
2. `%run ./00_fsa_common` added, and `add_fsa(df)` called immediately **after**
   `ClassificationPrep(df)` — `ClassificationPrep` does not touch the FSA columns, so every
   other variable's banding is untouched.
3. Normalization: upper-case + trim, nulls and blanks -> `"UNKNOWN"` rather than being
   dropped by the group-by. Optional `FSA_STRICT` bucket for values that fail the
   letter-digit-letter pattern (off by default).
4. A reconciliation cell at the end of each notebook, rolling the FSA output back up to the
   original grain so it can be tied to the territory run.

The ELR notebook's header (imports, widgets, the `spark.sql` read) was not in the issue
thread; it is reconstructed from the inforce notebook, which reads the same table.

## Verification

```bash
python tests/run_notebooks.py
```

Runs all three notebooks against a pandas-backed shim of the Spark API with synthetic
fixtures — 22 checks. It is not Spark and does not replace a cluster run; see
[AUDIT.md](AUDIT.md) §4 for exactly what it does and does not prove.

## Audit

[AUDIT.md](AUDIT.md) reviews the original extracts against the pipeline source and schema
dumps. One blocker (the TRX source table does not exist), six correctness fixes, two
deliberate non-changes, and seven things that need checking on the cluster.

## Before the first run

- **`veh_fsa_tx` must be on the TRX table.** The inforce pipeline does `select *`, so it is
  there. The TRX pipeline selects only the columns listed in
  `ap_trx_data_extract_helper.csv` — if `veh_fsa_tx` is not in that list, add it and re-run
  `06_ap_ppa_trx_pipeline.py`. `add_fsa()` raises with this message rather than failing
  deep in the aggregation.
- **`veh_fsa_tx` vs `pol_fsa_tx`.** The default is `veh_fsa_tx`, the vehicle garaging FSA —
  the geography `ter_onlvl_ibc_no` is itself derived from, so it is the like-for-like swap.
  `pol_fsa_tx` (policy mailing FSA) is also available; set it once in `00_fsa_common.py` so
  all three exhibits stay consistent.
- **Cell counts and credibility.** NS has roughly 80 live FSAs against a much smaller
  territory count, so cells are thinner by an order of magnitude and thinner still once
  split by company and accident year in the TRX cut. These extracts do no suppression or
  credibility grouping — that is a downstream decision, but do not read a raw FSA `LR_TOT`
  off a handful of exposures as a signal.
- **Data-out.** The extracts go out under the existing ADIDO tickets (3868 / 2569), whose
  approved column lists were written for the territory version. FSA is finer geography than
  territory; confirm it is covered before the first data-out.
