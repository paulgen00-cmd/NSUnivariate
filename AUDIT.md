# Audit of the three classification extracts

Reviewed against the pipeline source in `WT_REPO_issue18/` (notebooks 03, 04, 06), the PPOD
source-schema dumps for the trx and inforce views, and the catalog table registry in
`WT_REPO_docs/metadata/tables.csv`.

Findings are grouped by whether they are **fixed in this repo**, **left alone deliberately**,
or **cannot be settled from here** and need a check on the cluster.

---

## 1. Fixed — would have failed or produced wrong numbers

### 1.1 The TRX source table does not exist (blocker)

The original reads:

```
t_ap_ppa_pricing.trx_{province}_ppa_prep_onlvl_{as_of_date_suffix}
```

`06_ap_ppa_trx_pipeline.py` writes three things, and that is none of them:

| Table | Written at | Has `Prm_Ern_*_OnLvl_End_Am`? |
|---|---|---|
| `trx_{prov}_ppa_prep_{suffix}` | line 85 | no — prepped, not scored |
| `trx_{prov}_ppa_prep_{suffix}_onlvl` | `onlevel_premiums` appends `_onlvl` | no — scored, pre-split |
| `trx_ap_ppa_prep_onlvl_{suffix}` | line 235 | **yes** |

The suffix order differs (`prep_onlvl_{date}` vs `prep_{date}_onlvl`), and more importantly
the earned-premium and earned-exposure columns this extract sums are created *after* the
four provinces are unioned back together — the endorsement split at line 204 and the earning
loop at line 215-230. They exist only on the AP-wide table. `metadata/tables.csv` line 16
agrees. That the extract groups by `pol_jurisdiction_cd` at all is the other tell: a
province-specific table would have one value.

**Fix:** `resolve_table()` tries all three names and takes the first that exists *and* carries
every required column, then filters to the requested province. Correct whichever table your
workspace actually holds, and it names what it rejected if none work.

The inforce and ELR table names are **correct** — `inf_{prov}_ppa_prep_{YYYY_MM}_onlvl` is
what `onlevel_premiums` writes in `04_ap_ppa_inforce_pipeline.py` line 166-201, and the GA2M
loss costs and CLEAR on-level are joined in before the per-province write.

### 1.2 Ten silent renames in the ELR extract

`withColumnRenamed` on a column that does not exist is a **no-op in Spark — no error**. The
ELR notebook renames ten columns in a loop. If the scored table spells any one of them
differently, the loop succeeds, and the failure surfaces later as a missing
`Prm_OnLvl_*` column you never typed. **Fix:** `require_columns()` before the loop, listing
every missing name at once.

### 1.3 Null premium produced a null loss ratio

The LR guard tested `premium == 0`. A cell whose premium sums to null fell through it and
produced a null LR — sitting next to an `LC_*` that step 2 had already forced to 0 for
exactly that condition. The two branches disagreed. **Fix:** `isNull() | == 0` -> 0.0, so
both cases agree. Cells with genuine zero premium already returned 0 and are unaffected.

### 1.4 Rows with no FSA would vanish

`ter_onlvl_ibc_no` comes out of scoring and is populated. A raw FSA is not: `veh_fsa_tx` can
be null or blank, and a null group key silently drops those rows, so the FSA exhibit would
not tie to the territory one. **Fix:** null/blank collapse to a single `UNKNOWN` cell, plus a
coverage line printed before the aggregation and a warning above 1%.

### 1.5 Exposure loss from null coverage indicators

`cov_ap_col_in = cov_ap_in + cov_col_in`. If either operand is null the sum is null, and a
real exposure is dropped — where R's `na.rm = TRUE` would have counted it. **Fix:** coalesce
both to 0, which matches the R semantics. A diagnostic prints how many rows are affected; if
it prints 0 the change is a no-op and the output is byte-identical.

### 1.6 An AP-wide table would blend four provinces

The inforce and ELR extracts group by `pol_uwcompany_cd` and the variable only — no
jurisdiction in the key. Pointed at the AP-wide table they would silently sum NS, NB, NL and
PE into each cell. **Fix:** explicit province filter, and the TRX notebook asserts one
jurisdiction in the output.

### 1.7 Double-counted AP exposure if `dataprep` did not run

`cov_ap_col_in` is only 0/1 because `dataprep` zeroes `cov_col_in` when `cov_ap_in = 1`
(`03_atlantic_region_functions.py` line 592). On a table that skipped that step the
indicator reaches 2 and exposure doubles. **Fix:** assert that neither AP composite exceeds 1.

### 1.8 `fsa_coverage_check` cost three passes

`df.count()` plus a `groupBy().count()` plus a `collect()` — three scans of the trx table
before any real work. Rewritten as a single `agg`.

---

## 2. Left alone deliberately

### 2.1 TRX and inforce are on different row sets

The TRX extract filters `dri_type_cd != "OccasionalPrincipal"` **and** all six premiums
non-null. The inforce and ELR extracts build the identical `not_null_all_premiums` condition
— and never apply it. Only the driver-type filter runs.

This is in the original code and it is not obviously wrong: the inforce cut is a snapshot
where a null premium means "coverage not held", which is exactly what `Xpo_*` is counting.
But it does mean the TRX and inforce exhibits are not row-comparable, so a mix shift between
them may be an artefact of the filter rather than the book. **Unchanged** — matching the
published territory exhibits matters more than my opinion, and changing it silently would be
worse than either option. Flagging it for your call.

### 2.2 Capped and uncapped claim amounts are mixed

`clm_am_cols` uses `_cap500k_am` for TPL, BI, PD and AB but raw `_am` for DC, COL and CMP.
That looked like an oversight; it is not. The source view has **no** capped variant for DC,
COL or CMP — the only `cap500k` columns are TPL, BI, PD, AB and the three AB sub-perils.
Forced by the data. No action.

### 2.3 No suppression or credibility grouping

Deliberate — that is a downstream decision. But see §3.5.

---

## 3. Cannot be settled from here — check these before you trust a run

1. **`veh_fsa_tx` on the TRX table.** The inforce pipeline does `select *`; the TRX pipeline
   selects only what is listed in `ap_trx_data_extract_helper.csv`, which this repo cannot
   see. If it is absent, add it and re-run `06_ap_ppa_trx_pipeline.py`. `add_fsa()` raises
   with that instruction rather than dying inside the aggregation.
2. **`Prm_Trm_Dri_Tot_Uncap_Am`.** The LR_TOT denominator, and the one `rename_map` entry I
   could not confirm against any schema dump or notebook. `01_first_chance_discount.py`
   line 31 selects `Prm_Trm_Dri_Base_Uncap_Am` — Base, not Tot — from the same scored
   inforce output. If pyRate emits only Base, LR_TOT is measured against the wrong
   denominator. §1.2's guard names it explicitly.
3. **Which `ClassificationPrep` is deployed.** `03_atlantic_region_functions.py` defines it
   **twice**, and the versions differ: line 1112 adds `credit_band` on
   `clt_p_holder_credit_score_no` plus five interaction keys; line 1296 instead bands
   `exp_canc_nopay_03yrs_nb` and has no interactions. Whichever is at
   `/Functions/AP_PPA_Classification` is what runs. It does not affect the FSA key — but it
   does affect anyone who later un-comments the `VARS` list.
4. **`clt_p_holder_credit_score_no` is a StringType** in both source views, and `credit_band`
   compares it numerically. Spark will implicitly cast, so numeric strings work and anything
   else lands in `"NA"`. Fine today; would bite if the column ever carries a banded value.
5. **Cell counts.** NS has roughly 80 live FSAs against a handful of territories, and the TRX
   cut splits that again by company and accident year. `thin_cell_report()` prints how many
   output cells fall under an exposure floor. Do not read an `LR_TOT` off a dozen exposures.
6. **ADIDO scope.** Both tickets (3868, 2569) had their approved column lists written for the
   territory version. FSA is finer geography. Confirm it is covered before the first data-out.
7. **Relative `%run ./00_fsa_common`** resolves against the notebook's own folder. Keep the
   four files together, or switch to an absolute `/Workspace/...` path if you import them
   separately.

---

## 4. How this was verified

No cluster, so `tests/` runs the notebooks against a pandas-backed shim of the Spark API
(`tests/fakespark.py`) with synthetic fixtures:

```bash
python tests/run_notebooks.py
```

22 checks, all passing. It executes the real notebook source — it does not re-implement it —
and covers: the table resolution falling through to the AP-wide table, the province filter,
the `OccasionalPrincipal` and null-premium filters, case/whitespace FSA merging, the UNKNOWN
bucket, sum correctness per cell, the LR arithmetic including the zero-premium cell, the
ADIDO ticket and freeForm values, and all four guard paths firing (missing `veh_fsa_tx`,
missing rename column, overlapping AP/COL, no usable table).

**What it does not prove:** it is not Spark. No type checking, no catalog semantics, no
`%run` resolution, no pyRate, no ADIDO plumbing, and null handling in the corners will
differ. A green run means the dataframe logic is coherent. The cluster run is still the gate.
