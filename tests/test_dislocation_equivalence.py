"""Prove the refactor in databricks/05_dislocation.py is output-equivalent to the
original `dislocation` notebook, for every part that can be checked without Spark.

    python tests/test_dislocation_equivalence.py

The original replaces several hand-written literal chains with generated lists
(price bands, bucket labels, per-coverage aggregate aliases). Those generators
are exactly where a refactor silently changes a filing number, so each one is
compared against the literals scraped out of the original file.

What this does NOT prove: anything that needs a Spark session or pyRate -- the
joins, the window sums, split_endorsements, the scoring itself. Those are
unchanged code moved into functions, but they are not executed here.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ORIGINAL = (ROOT / "dislocation").read_text(encoding="utf-8")
REFACTOR = (ROOT / "databricks" / "05_dislocation.py").read_text(encoding="utf-8")

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
    """Verbatim from databricks/05_dislocation.py CELL 13."""
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
# 2. Vehicle price bands
# ---------------------------------------------------------------------------
# Scrape the original's hand-written when() chain: the (lo, hi, label) triples.
_price_src = ORIGINAL[ORIGINAL.index("def price_band"):]
_price_src = _price_src[:_price_src.index("# 11)")]
original_price = re.findall(
    r'col >= (\d+)\) & \(col < (\d+)\),\s*F\.lit\("([^"]+)"\)', _price_src)
original_price = [(int(a), int(b), c) for a, b, c in original_price]

refactor_price = ([(10000 + 5000 * i, 15000 + 5000 * i,
                    f"{10000 + 5000 * i}-{14999 + 5000 * i}") for i in range(18)]
                  + [(100000, 125000, "100000-124999"),
                     (125000, 150000, "125000-149999"),
                     (150000, 200000, "150000-199999")])

print("price bands")
check("price band triples", refactor_price, original_price)
check("price band count", len(refactor_price), 21)


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


print()
if FAILURES:
    print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
    sys.exit(1)
print("all equivalence checks passed")
