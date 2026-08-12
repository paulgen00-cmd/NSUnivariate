"""Execute the three classification notebooks against synthetic data via the fakespark shim.

    python tests/run_notebooks.py

Every row of the fixtures is chosen to exercise something that could plausibly break the
real run: mixed-case and blank FSAs, an OccasionalPrincipal row that must be filtered, a
null premium, a second province that must not leak in, a province-named TRX table that is
missing the earned-premium columns, and a cell whose premium sums to zero.

See fakespark.py for what this harness does and does not prove.
"""
import re
import sys
import traceback
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import fakespark as fs

fs.install()  # put the shim behind `import pyspark...` before any notebook is exec'd

ROOT = Path(__file__).resolve().parent.parent
NB = ROOT / "databricks"

FAILURES = []


# ---------------------------------------------------------------------------
# Notebook loading: strip Databricks magics, keep the python
# ---------------------------------------------------------------------------
def notebook_source(path: Path) -> str:
    """The notebooks are plain python plus `%run` magics; blank those out."""
    return "\n".join(
        "" if line.strip().startswith("%") else line
        for line in path.read_text(encoding="utf-8").split("\n")
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
TRX_PREM = ["Prm_Ern_BI_OnLvl_End_Am", "Prm_Ern_PD_OnLvl_End_Am", "Prm_Ern_DC_OnLvl_End_Am",
            "Prm_Ern_AB_OnLvl_End_Am", "Prm_Ern_AP_COL_OnLvl_End_Am",
            "Prm_Ern_AP_SP_CMP_OnLvl_End_Am"]
TRX_XPO = ["Xpo_Ern_BI_Nb", "Xpo_Ern_PD_Nb", "Xpo_Ern_DC_Nb", "Xpo_Ern_AB_Nb",
           "Xpo_Ern_AP_COL_Nb", "Xpo_Ern_AP_SP_CMP_Nb"]
TRX_CLM_NB = ["clm_chap_tpl_nb", "clm_chap_bi_nb", "clm_chap_dc_nb", "clm_chap_pd_nb",
              "clm_chap_ab_nb", "clm_chap_col_nb", "clm_chap_cmp_nb"]
TRX_CLM_AM = ["clm_chap_tpl_cap500k_am", "clm_chap_bi_cap500k_am", "clm_chap_pd_cap500k_am",
              "clm_chap_dc_am", "clm_chap_ab_cap500k_am", "clm_chap_col_am", "clm_chap_cmp_am"]

INF_PREM = ["Prm_Trm_BI_Uncap_Am", "Prm_Trm_DC_Uncap_Am", "Prm_Trm_PD_Uncap_Am",
            "Prm_Trm_AB_Uncap_Am", "Prm_Trm_AP_COL_Uncap_Am", "Prm_Trm_AP_SP_CMP_Uncap_Am"]
INF_EXTRA_PREM = ["Prm_Trm_UA_Uncap_Am", "Prm_Trm_SP_Uncap_Am", "Prm_Trm_UI_Uncap_Am",
                  "Prm_Trm_Dri_Tot_Uncap_Am"]
LC_SRC = ["lc_losscost_bi_pred_am", "lc_losscost_pd_pred_am", "lc_losscost_dc_pred_am",
          "lc_losscost_ab_pred_am", "lc_losscost_col_pred_am", "lc_losscost_cmp_pred_am",
          "lc_losscost_total_pred_am"]

# fsa, jurisdiction, company, driver type  — the shape of the book we feed in
ROWS = [
    ("B3H",   "NS", "snic", "Principal"),
    ("b3h ",  "NS", "snic", "Principal"),            # same cell as above once normalized
    ("B3J",   "NS", "prim", "Principal"),
    (None,    "NS", "other", "Principal"),           # -> UNKNOWN
    ("",      "NS", "other", "Principal"),           # -> UNKNOWN
    ("B4K",   "NS", "other", "OccasionalPrincipal"),  # filtered out
    ("E1C",   "NB", "snic", "Principal"),            # wrong province, must not leak
]


def trx_frame(with_earned: bool) -> pd.DataFrame:
    rows = []
    for i, (fsa, juris, comp, dtype) in enumerate(ROWS):
        r = {
            "veh_fsa_tx": fsa, "pol_jurisdiction_cd": juris, "pol_uwcompany_cd": comp,
            "dri_type_cd": dtype, "pol_acc_yr_dt": "2025",
            "ter_onlvl_ibc_no": 3,
        }
        if with_earned:
            for c in TRX_PREM:
                r[c] = 100.0 + i
            for c in TRX_XPO:
                r[c] = 1.0
            for c in TRX_CLM_NB:
                r[c] = 1.0
            for c in TRX_CLM_AM:
                r[c] = 500.0
        rows.append(r)
    pdf = pd.DataFrame(rows)
    if with_earned:
        # a null premium row must be dropped by not_null_all_premiums
        extra = pdf.iloc[0].copy()
        extra["veh_fsa_tx"] = "B9Z"
        extra["Prm_Ern_BI_OnLvl_End_Am"] = None
        pdf = pd.concat([pdf, extra.to_frame().T], ignore_index=True)
    return pdf


def inf_frame() -> pd.DataFrame:
    rows = []
    for i, (fsa, juris, comp, dtype) in enumerate(ROWS):
        r = {
            "veh_fsa_tx": fsa, "pol_jurisdiction_cd": juris, "pol_uwcompany_cd": comp,
            "dri_type_cd": dtype, "ter_onlvl_ibc_no": 3,
            "cov_bi_in": 1, "cov_dc_in": 1, "cov_pd_in": 1, "cov_ab_in": 1,
            "cov_ap_in": 0, "cov_col_in": 1, "cov_spe_in": 0, "cov_cmp_in": 1,
        }
        for c in INF_PREM + INF_EXTRA_PREM:
            r[c] = 200.0 + i
        for c in LC_SRC:
            r[c] = 50.0 + i
        rows.append(r)
    # a cell whose premium is zero -> LR must be 0, not a divide-by-zero
    z = dict(rows[2])
    z["veh_fsa_tx"] = "B0Z"
    for c in INF_PREM + INF_EXTRA_PREM:
        z[c] = 0.0
    rows.append(z)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Environment the notebooks run inside
# ---------------------------------------------------------------------------
def make_env(widgets):
    spark = fs.FakeSpark()
    adido_calls = []

    def adido_out(table=None, ticket=None, filename=None, freeForm=None,
                  fileformat="parquet", folder_out=None, **kw):
        adido_calls.append({"ticket": ticket, "freeForm": freeForm,
                            "folder_out": folder_out, "rows": table.count()})

    def ClassificationPrep(df):
        """Stand-in for /Functions/AP_PPA_Classification.

        The real one bands ~18 rating variables. What matters to these notebooks is only
        that it returns a frame and does NOT touch the FSA columns, so this asserts that
        contract and passes the frame through.
        """
        assert "veh_fsa_tx" in df.columns, "ClassificationPrep lost veh_fsa_tx"
        return df

    env = {
        "F": fs.F, "DataFrame": fs.DataFrame, "AnalysisException": fs.AnalysisException,
        "spark": spark, "dbutils": fs.FakeDbutils(widgets),
        "adido_out": adido_out, "ClassificationPrep": ClassificationPrep,
        "display": lambda x: None, "__name__": "notebook",
    }
    return env, spark, adido_calls


def run_nb(name, env):
    exec(compile(notebook_source(NB / name), name, "exec"), env)
    return env


def check(label, cond, detail=""):
    if cond:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}  {detail}")
        FAILURES.append(label)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_trx():
    print("\n01_trx_classification_fsa.py")
    env, spark, adido = make_env({"as_of_date_suffix": "202603", "province": "NS"})
    # the province-named table exists but is the PRE-earning one: no Prm_Ern_* columns.
    spark.register("t_ap_ppa_pricing.trx_ns_ppa_prep_202603_onlvl", trx_frame(with_earned=False))
    spark.register("t_ap_ppa_pricing.trx_ap_ppa_prep_onlvl_202603", trx_frame(with_earned=True))

    run_nb("01_trx_classification_fsa.py", env)
    out = env["df_out"].toPandas()

    check("resolved past the province table to the AP-wide one",
          env["trx_table"].endswith("trx_ap_ppa_prep_onlvl_202603"), env["trx_table"])
    check("NB rows excluded", set(out["pol_jurisdiction_cd"]) == {"NS"},
          str(set(out["pol_jurisdiction_cd"])))
    check("OccasionalPrincipal excluded", "B4K" not in set(out["fsa_tx"]))
    check("null-premium row excluded", "B9Z" not in set(out["fsa_tx"]))
    check("case/whitespace variants merged into one B3H cell",
          list(out["fsa_tx"]).count("B3H") == 1, str(sorted(out["fsa_tx"])))
    check("null and blank FSA collapse to a single UNKNOWN cell",
          list(out["fsa_tx"]).count("UNKNOWN") == 1, str(sorted(out["fsa_tx"])))
    check("B3H premium = sum of its two source rows",
          float(out.loc[out["fsa_tx"] == "B3H", "Prm_Ern_BI_OnLvl_End_Am"].iloc[0]) == 201.0,
          str(out.loc[out["fsa_tx"] == "B3H", "Prm_Ern_BI_OnLvl_End_Am"].tolist()))
    check("exposure ties to the filtered row count",
          out["Xpo_Ern_BI_Nb"].sum() == 5.0, str(out["Xpo_Ern_BI_Nb"].sum()))
    check("every measure column present in the output",
          all(c in out.columns for c in TRX_PREM + TRX_XPO + TRX_CLM_NB + TRX_CLM_AM))
    check("ADIDO called once, ticket 3868, freeForm TRX_fsa_tx",
          len(adido) == 1 and adido[0]["ticket"] == 3868 and adido[0]["freeForm"] == "TRX_fsa_tx",
          str(adido))


def test_trx_missing_fsa():
    print("\n01 — TRX table without veh_fsa_tx must fail loudly")
    env, spark, _ = make_env({"as_of_date_suffix": "202603", "province": "NS"})
    pdf = trx_frame(with_earned=True).drop(columns=["veh_fsa_tx"])
    spark.register("t_ap_ppa_pricing.trx_ap_ppa_prep_onlvl_202603", pdf)
    try:
        run_nb("01_trx_classification_fsa.py", env)
        check("raises when veh_fsa_tx is absent", False, "no exception")
    except ValueError as e:
        check("raises when veh_fsa_tx is absent, naming the column",
              "veh_fsa_tx" in str(e), str(e)[:160])


def test_inforce():
    print("\n02_inforce_classification_fsa.py")
    env, spark, adido = make_env({"as_of_date": "2026_03", "province": "NS"})
    spark.register("t_ap_ppa_pricing.inf_ns_ppa_prep_2026_03_onlvl", inf_frame())

    run_nb("02_inforce_classification_fsa.py", env)
    out = env["df_out"].toPandas()

    check("used the province-named inforce table",
          env["inf_table"].endswith("inf_ns_ppa_prep_2026_03_onlvl"), env["inf_table"])
    check("OccasionalPrincipal excluded", "B4K" not in set(out["fsa_tx"]))
    check("company codes mapped to SN/PIC/TDHA",
          set(out["pol_uwcompany_cd"]) <= {"SN", "PIC", "TDHA"}, str(set(out["pol_uwcompany_cd"])))
    check("AP_COL exposure is 0/1 per row, not double counted",
          out["Xpo_AP_COL"].sum() == 6.0, str(out["Xpo_AP_COL"].sum()))
    check("B3H exposure = 2 vehicles", int(out.loc[out["fsa_tx"] == "B3H", "Xpo_BI"].iloc[0]) == 2,
          str(out.loc[out["fsa_tx"] == "B3H", "Xpo_BI"].tolist()))
    check("ADIDO ticket 2569, freeForm Inforce_fsa_tx",
          adido and adido[0]["ticket"] == 2569 and adido[0]["freeForm"] == "Inforce_fsa_tx",
          str(adido))


def test_inforce_double_count_guard():
    print("\n02 — AP/COL overlap must trip the double-count assert")
    env, spark, _ = make_env({"as_of_date": "2026_03", "province": "NS"})
    pdf = inf_frame()
    pdf.loc[0, "cov_ap_in"] = 1          # AP and COL both on -> exposure would be 2
    spark.register("t_ap_ppa_pricing.inf_ns_ppa_prep_2026_03_onlvl", pdf)
    try:
        run_nb("02_inforce_classification_fsa.py", env)
        check("assert fires on overlapping AP/COL", False, "no exception")
    except AssertionError as e:
        check("assert fires on overlapping AP/COL", "double count" in str(e), str(e)[:120])


def test_elr():
    print("\n03_elr_classification_fsa.py")
    env, spark, adido = make_env({"as_of_date": "2026_03", "province": "NS"})
    spark.register("t_ap_ppa_pricing.inf_ns_ppa_prep_2026_03_onlvl", inf_frame())

    run_nb("03_elr_classification_fsa.py", env)
    out = env["df_out"].toPandas()

    check("premium columns renamed to Prm_OnLvl_*",
          "Prm_OnLvl_BI_Uncap_Am" in out.columns, str(list(out.columns)[:6]))
    check("all seven LR columns produced",
          all(f"LR_{c}" in out.columns for c in ["BI", "PD", "DC", "AB", "COL", "CMP", "TOT"]))
    b3h = out[out["fsa_tx"] == "B3H"].iloc[0]
    expected = (50.0 + 51.0) / (200.0 + 201.0)
    check("LR_BI = sum(LC) / sum(premium) for the merged B3H cell",
          abs(float(b3h["LR_BI"]) - expected) < 1e-9,
          f"{b3h['LR_BI']} vs {expected}")
    zero = out[out["fsa_tx"] == "B0Z"].iloc[0]
    check("zero-premium cell gives LR = 0, not inf/NaN",
          float(zero["LR_TOT"]) == 0.0, str(zero["LR_TOT"]))
    check("ADIDO ticket 2569, freeForm ELR_Inf_fsa_tx",
          adido and adido[0]["ticket"] == 2569 and adido[0]["freeForm"] == "ELR_Inf_fsa_tx",
          str(adido))


def test_elr_missing_rename_col():
    print("\n03 — a missing rename_map column must fail before the silent no-op bites")
    env, spark, _ = make_env({"as_of_date": "2026_03", "province": "NS"})
    pdf = inf_frame().drop(columns=["Prm_Trm_Dri_Tot_Uncap_Am"])
    spark.register("t_ap_ppa_pricing.inf_ns_ppa_prep_2026_03_onlvl", pdf)
    try:
        run_nb("03_elr_classification_fsa.py", env)
        check("raises naming Prm_Trm_Dri_Tot_Uncap_Am", False, "no exception")
    except ValueError as e:
        check("raises naming Prm_Trm_Dri_Tot_Uncap_Am",
              "Prm_Trm_Dri_Tot_Uncap_Am" in str(e), str(e)[:160])


def test_no_table():
    print("\nresolve_table — no usable candidate must say why")
    env, spark, _ = make_env({"as_of_date_suffix": "202603", "province": "NS"})
    spark.register("t_ap_ppa_pricing.trx_ns_ppa_prep_202603_onlvl", trx_frame(with_earned=False))
    try:
        run_nb("01_trx_classification_fsa.py", env)
        check("raises listing what was tried", False, "no exception")
    except ValueError as e:
        msg = str(e)
        check("raises listing what was tried",
              "no usable TRX table" in msg and "missing" in msg, msg[:200])


if __name__ == "__main__":
    for t in (test_trx, test_trx_missing_fsa, test_inforce, test_inforce_double_count_guard,
              test_elr, test_elr_missing_rename_col, test_no_table):
        try:
            t()
        except Exception:
            print(f"  ERROR in {t.__name__}")
            traceback.print_exc()
            FAILURES.append(t.__name__)

    print("\n" + "=" * 60)
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
        sys.exit(1)
    print("all checks passed")
