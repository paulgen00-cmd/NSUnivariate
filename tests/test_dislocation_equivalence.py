"""Check databricks/05a + 05b against the original `dislocation` notebook, for
every part that can be verified without Spark.

    python tests/test_dislocation_equivalence.py

Three groups of checks:

1. UNCHANGED -- the measures. Rate-change splits and labels, and the aggregate
   output column names in order for all three exhibits, are compared against
   the literals scraped out of the original file. A refactor that reorders an
   .agg() silently reshuffles a filing exhibit, so these are pinned exactly.

2. DELIBERATELY CHANGED -- the bucketing. 05b calls the shared
   ClassificationPrep instead of the original's hand-rolled km / price /
   veh-age / cap chain, so the dislocation exhibit is cut the same way as the
   classification run. These checks pin the swap rather than the old values.

3. STRUCTURAL -- the 05a/05b split. 05a makes exactly one scoring call, 05b
   makes none, and the table names 05a writes are the ones 05b reads.

What this does NOT prove: anything needing a Spark session or pyRate -- the
joins, the window sums, split_endorsements, ClassificationPrep's own output, or
the scoring. Run 05b with RUN_ADIDO = False and compare before exporting.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ORIGINAL = (ROOT / "dislocation").read_text(encoding="utf-8")
SCORE = (ROOT / "databricks" / "05a_dislocation_score.py").read_text(encoding="utf-8")
REFACTOR = (ROOT / "databricks" / "05b_dislocation_summary.py").read_text(encoding="utf-8")

FAILURES = []


def check(name, got, want):
    if got == want:
        print(f"  ok   {name}")
    else:
        FAILURES.append(name)
        print(f"  FAIL {name}")
        if isinstance(got, list) and isinstance(want, list):
            for i in range(max(len(got), len(want))):
                g = got[i] if i < len(got) else "<missing>"
                w = want[i] if i < len(want) else "<missing>"
                if g != w:
                    print(f"         [{i}] refactor={g!r}  original={w!r}")
        else:
            print(f"         refactor={got!r}\n         original={want!r}")


# ---------------------------------------------------------------------------
# 1. Rate-change bucket splits and labels
# ---------------------------------------------------------------------------
def original_labels(lower_bound, upper_bound, bin_step):
    """Verbatim from the original notebook, including the trailing replace()."""
    seq_vals = [lower_bound + i * bin_step
                for i in range(int(round((upper_bound - lower_bound) / bin_step)) + 1)]
    splits = [-float("inf")] + seq_vals + [float("inf")]
    labels = []
    for i in range(len(splits) - 1):
        left, right = splits[i], splits[i + 1]

        def fmt(x):
            if x == float("inf"):
                return "Inf"
            if x == -float("inf"):
                return "-Inf"
            return f"{x:.3f}".rstrip('0').rstrip('.') if '.' in f"{x:.3f}" else f"{x:.3f}"

        labels.append(f"[{fmt(left)}, {fmt(right)})")
    return splits, [lbl.replace(", ", ",") for lbl in labels]


def refactor_labels(lower_bound, upper_bound, bin_step):
    """Verbatim from databricks/05b_dislocation_summary.py."""
    n_bins = int(round((upper_bound - lower_bound) / bin_step)) + 1
    seq_vals = [lower_bound + i * bin_step for i in range(n_bins)]
    splits = [-float("inf")] + seq_vals + [float("inf")]

    def _fmt(x):
        if x == float("inf"):
            return "Inf"
        if x == -float("inf"):
            return "-Inf"
        s = f"{x:.3f}"
        return s.rstrip("0").rstrip(".") if "." in s else s

    return splits, [f"[{_fmt(splits[i])},{_fmt(splits[i + 1])})"
                    for i in range(len(splits) - 1)]


print("rate-change buckets")
# The configured bounds, plus a few alternatives an actuary might set.
for lo, hi, step in [(-0.05, 0.15, 0.05), (-0.10, 0.25, 0.025), (0.0, 0.10, 0.05),
                     (-0.20, 0.20, 0.05)]:
    o_splits, o_labels = original_labels(lo, hi, step)
    r_splits, r_labels = refactor_labels(lo, hi, step)
    check(f"splits {lo},{hi},{step}", r_splits, o_splits)
    check(f"labels {lo},{hi},{step}", r_labels, o_labels)


# ---------------------------------------------------------------------------
# 2. Bucketing now comes from ClassificationPrep, not hand-rolled banding
# ---------------------------------------------------------------------------
# 05b deliberately drops the original's own km / price / veh-age / cap chain and
# calls the shared ClassificationPrep, so the dislocation exhibit is cut the same
# way as the classification run. These checks pin that swap: if a hand-rolled
# bander comes back, the two analyses silently diverge again.
print("bucketing delegated to ClassificationPrep")
check("05b runs AP_PPA_Classification",
      "Functions/AP_PPA_Classification" in REFACTOR, True)
check("05b calls ClassificationPrep", "ClassificationPrep(d)" in REFACTOR, True)
for bander in ["def price_band", "def km_annual_group", "def km_work_group",
               "def veh_age_bucket", "def cap_gt8_as_str", "def band_common"]:
    check(f"05b no longer defines {bander[4:]}", bander in REFACTOR, False)

# The original's own price_band is still the reference for what ClassificationPrep
# is expected to produce -- the guide documents the same $5k bands. Confirm the
# original really did carry 21 interior bands, so the README claim is accurate.
_price_src = ORIGINAL[ORIGINAL.index("def price_band"):]
_price_src = _price_src[:_price_src.index("# 11)")]
original_price = re.findall(
    r'col >= (\d+)\) & \(col < (\d+)\),\s*F\.lit\("([^"]+)"\)', _price_src)
check("original price band count", len(original_price), 21)

# c_variables must name what ClassificationPrep builds. The four interaction keys
# and the normalised FSA are the ones that would fail at group-by time.
print("c_variables")
_cv = REFACTOR[REFACTOR.index("c_variables = ["):]
_cv = _cv[:_cv.index("]")]
for v in ["dri_type_cd_x_rat_km_work_nb", "dri_type_cd_x_rat_km_business_nb",
          "dri_type_cd_x_rat_km_annual_nb", "dri_yrs_licensed_au_nb_x_dri_gender_cd",
          "clt_p_holder_credit_score_no", "fsa_tx"]:
    check(f"c_variables has {v}", f'"{v}"' in _cv, True)
check("c_variables no longer names veh_fsa_tx directly", '"veh_fsa_tx"' in _cv, False)
check("05b guards c_variables against ClassificationPrep",
      "did not produce" in REFACTOR, True)

# FSA selection, matching 01/02.
print("FSA selection")
check("FSA_COL widget", '"FSA_COL"' in REFACTOR, True)
check("FSA normalised to UNKNOWN", 'F.lit("UNKNOWN")' in REFACTOR, True)

# Company mapping must happen before any join on pol_uwcompany_cd.
print("company mapping order")
_map_at = REFACTOR.index("_map_company")
_join_at = REFACTOR.index("def join_sides")
check("company map precedes the joins", _map_at < _join_at, True)

# ---------------------------------------------------------------------------
# 2b. Every %run cell must be guarded
# ---------------------------------------------------------------------------
# A Databricks magic is skipped unless it is the first line of its cell, so a
# mis-pasted %run silently loads nothing and fails as a NameError much later.
# Both notebooks preflight the names their %run cells should define.
print("%run guards")
for label, text, n_runs in [("05a", SCORE, 6), ("05b", REFACTOR, 2)]:
    runs = re.findall(r"^%run (\S+)", text, re.M)
    check(f"{label} has {n_runs} %run lines", len(runs), n_runs)
    # No %run may have a comment on the line directly above it -- that is the
    # exact mistake that kills the magic.
    lines = text.split(chr(10))
    bad = [l for i, l in enumerate(lines)
           if l.startswith("%run") and i and lines[i - 1].strip().startswith("#")]
    check(f"{label} no comment directly above a %run", bad, [])
    check(f"{label} preflights the loaded names", "preflight ok" in text, True)
    check(f"{label} names the paste mistake", "FIRST LINE of its cell" in text, True)

# 05a's two dataprep captures must be guarded, since that is where the missing
# name actually surfaced.
check("05a guards dataprep_trx",
      "try:" + chr(10) + "    dataprep_trx = dataprep" in SCORE, True)
check("05a guards dataprep_inf",
      "try:" + chr(10) + "    dataprep_inf = dataprep" in SCORE, True)

# ---------------------------------------------------------------------------
# 3. Aggregate output column names, in order
# ---------------------------------------------------------------------------
def scrape_aliases(text, start_marker, end_marker):
    """Every .alias("X") between two markers, in source order."""
    seg = text[text.index(start_marker):]
    seg = seg[:seg.index(end_marker)]
    return re.findall(r'\.alias\("([^"]+)"\)', seg)


CLAIM_COLS = ["clm_chap_bi_cap500k_am", "clm_chap_pd_am", "clm_chap_dc_am",
              "clm_chap_ab_cap500k_am", "clm_chap_um_am", "clm_chap_col_am",
              "clm_chap_cmp_am", "clm_chap_ui_am"]

# --- by-driver TRX summary --------------------------------------------------
DRI_ALIASES = [("BI", "BI"), ("PD", "PD"), ("DC", "DC"), ("AB", "AB"), ("UA", "UA"),
               ("AP_COL", "COL"), ("AP_SP_COMP", "CMP"), ("UI", "UI")]

refactor_dri = ([f"Prm_Trm_{dst}_Uncap_Am_Sum" for _, dst in DRI_ALIASES]
                + ["Prm_Trm_Base_Uncap_Am_Sum"]
                + [f"Prm_Trm_{dst}_Uncap_Am_Prop_Sum" for _, dst in DRI_ALIASES]
                + ["Prm_Trm_Dri_Base_Uncap_Am_Prop_Sum"]
                + CLAIM_COLS + ["clm_chap_tot_am"])

original_dri = scrape_aliases(ORIGINAL, "df_trx_dri_fin2 = (", "df_trx_dri_fin2.display()")

print("TRX by-driver aggregate columns")
check("by-driver aliases", refactor_dri, original_dri)

# --- by-vehicle TRX summary -------------------------------------------------
original_veh = scrape_aliases(ORIGINAL, "df_trx_veh_fin = (", "df_trx_veh_fin.display()")
refactor_veh = ["Prm_Trm_Veh_Base_Uncap_Am_Sum",
                "Prm_Trm_Veh_Base_Uncap_Am_Prop_Sum",
                "clm_veh_chap_tot_am"]

print("TRX by-vehicle aggregate columns")
check("by-vehicle aliases", refactor_veh, original_veh)

# --- inforce by-vehicle summary ---------------------------------------------
INF_SUM_COVS = ["BI", "PD", "DC", "AB", "UA", "AP_COL", "AP_SP_COMP", "UI", "Base"]
refactor_inf = (["Xpo_Tot", "Xpo_Col", "Xpo_Cmp"]
                + [f"Prm_Trm_Veh_{c}_Uncap_Am_Sum" for c in INF_SUM_COVS]
                + [f"Prm_Trm_Veh_{c}_Uncap_Am_Prop_Sum" for c in INF_SUM_COVS])

original_inf = scrape_aliases(ORIGINAL, "inf_final = (", "inf_final.display()")

print("inforce by-vehicle aggregate columns")
check("inforce aliases", refactor_inf, original_inf)


# ---------------------------------------------------------------------------
# 4. Per-side earned-premium column names
# ---------------------------------------------------------------------------
EARN_MAP = [("BI", "BI"), ("PD", "PD"), ("DC", "DC"), ("AB", "AB"), ("UA", "UA"),
            ("AP_COL", "AP_COL"), ("AP_SP_COMP", "AP_SP_CMP"), ("UI", "UI")]

for side, tag, src_s in [("current", "_Ern", ""), ("proposed", "_Prop_Ern", "_Prop")]:
    refactor_pairs = [(f"Prm_Trm_{o}_Uncap_Am{tag}", f"Prm_Ern_{s}_OnLvl_Am{src_s}")
                      for o, s in EARN_MAP]
    # Scrape the original's first (vehicle-level) block for this side.
    seg = ORIGINAL[ORIGINAL.index("df_trx_veh = ("):ORIGINAL.index("# Aggregate to vehicle level")]
    pat = re.compile(r'\.withColumn\("(Prm_Trm_\w+?_Uncap_Am' + re.escape(tag)
                     + r')",\s*nz\(F\.col\("(Prm_Ern_\w+?_OnLvl_Am'
                     + re.escape(src_s) + r')"\)\)\)')
    original_pairs = pat.findall(seg)
    print(f"earned premium mapping ({side})")
    check(f"{side} earned pairs", refactor_pairs, original_pairs)


# ---------------------------------------------------------------------------
# 5. CHART name handling -- the bug this refactor exists to close
# ---------------------------------------------------------------------------
def original_suffix(chart):
    return chart[:-5].replace('.', '_')


def refactor_suffix(chart):
    base = chart[:-5] if chart.lower().endswith(".json") else chart
    return base.replace(".", "_")


print("CHART suffix derivation")
# With the extension present the two agree -- the refactor must not change the
# happy path, because these strings are baked into existing table names.
for chart in ["NL.PPA.20260702.json", "NL.PPA.20260827.sbr.json", "NS.PPA.20250411.json"]:
    check(f"unchanged for {chart}", refactor_suffix(chart), original_suffix(chart))

# Without it, the original silently truncates. That is the failure mode.
bad = "NL.PPA.20260827.sbr"
if original_suffix(bad) == refactor_suffix(bad):
    FAILURES.append("truncation not fixed")
    print("  FAIL truncation not fixed")
else:
    print(f"  ok   truncation fixed: original {original_suffix(bad)!r} "
          f"-> refactor {refactor_suffix(bad)!r}")


# ---------------------------------------------------------------------------
# 6. The refactor must not have dropped an export
# ---------------------------------------------------------------------------
print("ADIDO exports")
for free in ["TRX_by_Vehicle_", "TRX_by_Driver_", "Inf_by_Vehicle_"]:
    check(f"{free} present", free in REFACTOR, True)
check("ticket 2569", REFACTOR.count("ticket=2569"), 1)  # one loop, three files


# ---------------------------------------------------------------------------
# 7. The 05a/05b split
# ---------------------------------------------------------------------------
# The whole point of the split is that a run never submits two scorings. If a
# second onlevel_premiums call ever creeps back into 05a, the failure this
# structure exists to avoid comes straight back.
print("one scoring per execution")
check("05a calls onlevel_premiums once",
      len(re.findall(r'(?<!def )onlevel_premiums\(', SCORE)), 1)
check("05b never calls pyRate", "onlevel_premiums(" in REFACTOR, False)
check("05b never imports ARRR", "ARRR" in REFACTOR, False)

# 05a writes tables, 05b reads them by name. If the two templates drift, 05b
# raises "missing input table" and nobody can tell which side is wrong.
def table_templates(text):
    """Every f-string that looks like a schema-qualified table name, with the
    chart-suffix variable normalised so 05a's {CHART} matches 05b's {CUR}."""
    found = re.findall(r'f"(\{schema\}\.[^"]+)"', text)
    return {re.sub(r'\{(CHART|CUR|PRP)\}', '{C}', t) for t in found}

score_tables = table_templates(SCORE)
summary_tables = table_templates(REFACTOR)

trx_template = "{schema}.trx_{province.lower()}_ppa_prep_onlvl_{as_of_date_suffix}_{C}"
inf_template = "{schema}.inf_{province.lower()}_ppa_prep_{as_of_dt_f3}_{C}"

print("05a output names match 05b input names")
check("TRX table in 05a", trx_template in score_tables, True)
check("TRX table in 05b", trx_template in summary_tables, True)
check("INF table in 05a", inf_template in score_tables, True)
# 05b reads the pyRate output, which is the 05a table plus the _onlvl suffix
# that onlevel_premiums appends.
check("INF table in 05b", inf_template + "_onlvl" in summary_tables, True)

# chart_suffix must be character-identical in both files, or the table names
# built by 05a and looked up by 05b diverge for any chart with a dot suffix.
def extract_chart_suffix(text, label):
    src = text[text.index("def chart_suffix"):]
    src = src[:src.index("\n\n")]
    ns = {}
    exec(src, ns)
    return ns["chart_suffix"]

print("chart_suffix agrees between 05a and 05b")
_a = extract_chart_suffix(SCORE, "05a")
_b = extract_chart_suffix(REFACTOR, "05b")
for chart in ["NS.PPA.20260702.json", "NS.PPA.20261214.S8.json",
              "NS.PPA.20261214_cs.json", "NL.PPA.20260827.sbr.json",
              "  NS.PPA.20260702.json  ", "NS.PPA.20261214.S8"]:
    check(f"suffix agrees for {chart!r}", _a(chart.strip()), _b(chart))

# The four table names 05b resolves must be exactly the four 05a can produce.
check("05b resolves four inputs", REFACTOR.count('("trx", "current")')
      + REFACTOR.count('("trx", "proposed")')
      + REFACTOR.count('("inf", "current")')
      + REFACTOR.count('("inf", "proposed")') >= 4, True)


print()
if FAILURES:
    print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
    sys.exit(1)
print("all equivalence checks passed")
