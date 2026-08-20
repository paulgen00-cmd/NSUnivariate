# NSUnivariate

FSA-level versions of the three Atlantic PPA classification extracts. Same outputs as the
production `ter_onlvl_ibc_no` runs — same filters, same measures, same ADIDO tickets — with
the grouping variable swapped from the IBC territory to the **raw FSA**.

Source of the originals: issue #1 of this repo (body = TRX, comments = ELR and Inforce).

## Notebooks (`databricks/`)

| File | Replaces | Grain | Output |
|---|---|---|---|
| `01_trx_classification_fsa.py` | TRX extract | jurisdiction x company x accident year x FSA | ADIDO 3868, `TRX_fsa_tx` |
| `02_inforce_classification_fsa.py` | Inforce extract | company x FSA | ADIDO 2569, `Inforce_fsa_tx` |
| `03_elr_classification_fsa.py` | ELR extract | company x FSA | ADIDO 2569, `ELR_Inf_fsa_tx` |
| `04_elr_all_variables_demand.py` | ELR extract, all variables | company x variable x level | ADIDO 2569, `ELR_Demand_AllVars` |
| `05a_dislocation_score.py` | part 1 of the root `dislocation` notebook | one dataset x one chart | a scored delta table |
| `05b_dislocation_summary.py` | part 2 of the root `dislocation` notebook | vehicle / driver x rate-change band | ADIDO 2569, three parquet files |

**Each file is three cells.** The two `%run` lines must each sit alone in their own cell —
Databricks rejects a `%run` that shares a cell with other code, and everything after it in
that cell never runs. Cell 3 is the whole rest of the file, pasted in one go. The widgets
create themselves with defaults, so nothing has to exist beforehand.

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

## Dislocation

`dislocation` (repo root) is Jessie Wu's original notebook: extract TRX and inforce,
on-level each against both the current and the proposed CHART, and summarise the
premium change by rate-change band. Four pyRate scorings, ~30 min.

### The `'NoneType' object has no attribute 'count'` crash

`test_merge_df(df, df_PROV_proposed)` fails because `onlevel_premiums` returned `None`.
It can do that without raising: the shared function polls until `{input}_onlvl` appears,
and its only exit *with* a DataFrame is the `break`. When the pyRate job reaches `Failed`
before the output table shows up, the `while` condition ends the loop and the last
`catch_onlvl` result — `None` — is returned. So the on-level cell looks like it finished
and the crash lands one cell later. **The scoring job failed; the `NoneType` is the
messenger.**

On the NS run that hit this, the proposed chart resolved fine — `available_charts`
listed `NS.PPA.20261214.S8.json` and `chart_reference` returned. What fails is the
**second scoring in one session**: the same chart scores successfully when it is the only
`onlevel_premiums` call in a run. That is why 05a/05b split along that line.

Two guards are in `dislocation` itself so the next failure of any kind reports itself
properly:

- `RUO_chart_*_1` was derived with `chart[:-5]`, which assumes the widget value ends in
  `.json`. Entered without the extension it chops five real characters off the name —
  the copy tables get a truncated suffix *and* the `chart=` argument never resolves.
  Suffix derivation now strips `.json` only if it is there, and both widget values are
  checked against `mclient.available_charts`. This was not the NS cause, but it is a live
  trap for any chart name typed without the extension.
- `onlevel_premiums_checked()` verifies the input table exists and is non-empty, resolves
  the chart *before* the long poll (a bad name now fails in seconds, not after a full
  scoring cycle), and raises with the chart and table named if the result is `None`.

### Run it as `05a` four times, then `05b` once

The original calls `onlevel_premiums` twice back to back in one session. The first
succeeds; the second reaches `Failed` within ~60 seconds. Scoring one chart per
execution works, so the pipeline is split along that line:

```
05a  dataset=trx  RUO_Chart=<current>    ->  trx_<prov>_ppa_prep_onlvl_<suffix>_<CUR>
05a  dataset=trx  RUO_Chart=<proposed>   ->  trx_<prov>_ppa_prep_onlvl_<suffix>_<PRP>
05a  dataset=inf  RUO_Chart=<current>    ->  inf_<prov>_ppa_prep_<f3>_<CUR>_onlvl
05a  dataset=inf  RUO_Chart=<proposed>   ->  inf_<prov>_ppa_prep_<f3>_<PRP>_onlvl
05b                                      ->  the three ADIDO 2569 files
```

**`05a_dislocation_score.py`** makes exactly one `onlevel_premiums` call — the test
suite enforces that, because a second one reintroduces the failure this split exists
to avoid. It resolves the chart against `available_charts` and calls `chart_reference`
*before* the poll, so a bad chart fails in seconds rather than after a scoring cycle,
and raises with the chart and table named if the result is `None`. It prints the raw
row count; that number must match between the current and proposed runs of the same
dataset.

**`05b_dislocation_summary.py`** calls pyRate zero times, so it runs in minutes and can
be re-run freely. It checks all four input tables exist — naming the exact 05a
invocation for any that are missing — and refuses to proceed if the current and
proposed row counts differ, which would mean the two runs saw different source data.

Everything else is the original logic, with the current/proposed duplication collapsed:
the original repeats the endorsement-split, earning and 8-coverage premium blocks two to
three times each.

Known behaviour carried over deliberately, **not** fixed:

- the inforce path does `dropDuplicates(veh_keys)` after filtering to `Principal`, which
  picks an arbitrary row when a vehicle has more than one principal-driver row
- TRX filters `veh_product_cd in ('ppa')` while inforce uses `('ppa','motor_home')`
- the `%skip` export cell references `test`, which is never defined

## Verification

```bash
python tests/run_notebooks.py
```

Runs all three notebooks against a pandas-backed shim of the Spark API with synthetic
fixtures — 22 checks. It is not Spark and does not replace a cluster run; see
[AUDIT.md](AUDIT.md) §4 for exactly what it does and does not prove.

```bash
python tests/test_dislocation_equivalence.py
```

Checks `05a`/`05b` against the original `dislocation` for every part that does not need
Spark: the rate-change splits and labels (at four bin settings), the 21 vehicle price
bands, and the aggregate output column names in order for all three exhibits — each
compared against the literals scraped out of the original file. It also enforces the
split itself: 05a makes exactly one scoring call, 05b makes none, and the table names
05a writes are the ones 05b reads, with `chart_suffix` identical in both. It does **not**
execute the joins, window sums, `split_endorsements`, or the scoring.

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
