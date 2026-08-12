"""A pandas-backed stand-in for the slice of the PySpark API these notebooks use.

WHAT THIS IS FOR
    Running the three classification notebooks end to end on a laptop, so that column-name
    typos, wrong aggregations, broken filters and bad LR arithmetic are caught before a
    cluster is booked. It executes the real notebook source — it does not re-implement it.

WHAT THIS IS NOT
    It is not Spark. It does not check types, catalog behaviour, partitioning, null
    semantics in the corners, `%run` resolution on the workspace, ADIDO plumbing, or
    anything about pyRate. A green run here means "the dataframe logic is coherent", not
    "this is safe to publish". The real cluster run is still the gate.
"""
import re
import pandas as pd


# ---------------------------------------------------------------------------
# Columns
# ---------------------------------------------------------------------------
class Col:
    def __init__(self, fn, name):
        self.fn = fn
        self.name = name

    def _lift(self, other):
        return other if isinstance(other, Col) else lit(other)

    def _bin(self, other, op, sym):
        o = self._lift(other)
        return Col(lambda df: op(self.fn(df), o.fn(df)), f"({self.name}{sym}{o.name})")

    def __eq__(self, o):  return self._bin(o, lambda a, b: a == b, "=")
    def __ne__(self, o):  return self._bin(o, lambda a, b: a != b, "!=")
    def __lt__(self, o):  return self._bin(o, lambda a, b: a < b, "<")
    def __gt__(self, o):  return self._bin(o, lambda a, b: a > b, ">")
    def __and__(self, o): return self._bin(o, lambda a, b: a & b, " and ")
    def __or__(self, o):  return self._bin(o, lambda a, b: a | b, " or ")
    def __add__(self, o): return self._bin(o, lambda a, b: a + b, "+")
    def __truediv__(self, o): return self._bin(o, lambda a, b: a / b, "/")

    def isNull(self):    return Col(lambda df: self.fn(df).isna(), f"{self.name} is null")
    def isNotNull(self): return Col(lambda df: self.fn(df).notna(), f"{self.name} not null")
    def isin(self, *v):
        vals = list(v[0]) if len(v) == 1 and isinstance(v[0], (list, tuple, set)) else list(v)
        return Col(lambda df: self.fn(df).isin(vals), f"{self.name} in {vals}")

    def rlike(self, pat):
        return Col(lambda df: self.fn(df).astype("string").str.match(pat).fillna(False),
                   f"{self.name} rlike {pat}")

    def cast(self, _t): return self
    def alias(self, n): return Col(self.fn, n)


class _When:
    """F.when(...).when(...).otherwise(...) — first match wins, like Spark."""

    def __init__(self, pairs, name="CASE"):
        self.pairs = pairs
        self.name = name

    def when(self, cond, val):
        return _When(self.pairs + [(cond, val)], self.name)

    def otherwise(self, val):
        pairs, default = self.pairs, val

        def run(df):
            out = _as_series(default, df).copy()
            # reverse order so the FIRST matching condition wins
            for cond, v in reversed(pairs):
                mask = cond.fn(df).fillna(False)
                out = out.mask(mask, _as_series(v, df))
            return out

        return Col(run, self.name)

    # a `when` with no `otherwise` yields null elsewhere
    @property
    def fn(self):
        return self.otherwise(None).fn


def _as_series(v, df):
    if isinstance(v, Col):
        return v.fn(df)
    if isinstance(v, _When):
        return v.otherwise(None).fn(df)
    return pd.Series([v] * len(df), index=df.index)


class Agg:
    def __init__(self, kind, col, name):
        self.kind, self.col, self.name = kind, col, name

    def alias(self, n):
        return Agg(self.kind, self.col, n)

    def apply(self, df):
        s = self.col.fn(df) if self.col is not None else pd.Series([1] * len(df), index=df.index)
        if self.kind == "sum":
            return s.sum(skipna=True)          # matches R's na.rm = TRUE
        if self.kind == "count":
            return int(s.notna().sum()) if self.col is not None else len(df)
        if self.kind == "countDistinct":
            return int(s.nunique(dropna=True))
        raise NotImplementedError(self.kind)


# ---------------------------------------------------------------------------
# functions namespace (F)
# ---------------------------------------------------------------------------
def col(name):
    def get(df):
        if name not in df.columns:
            raise KeyError(f"column not found: {name}")
        return df[name]
    return Col(get, name)


def lit(v):
    return Col(lambda df: pd.Series([v] * len(df), index=df.index), repr(v))


def when(cond, val):
    return _When([(cond, val)])


def upper(c):  return Col(lambda df: c.fn(df).astype("string").str.upper(), f"upper({c.name})")
def trim(c):   return Col(lambda df: c.fn(df).astype("string").str.strip(), f"trim({c.name})")


def coalesce(*cols):
    def run(df):
        out = cols[0].fn(df)
        for c in cols[1:]:
            out = out.fillna(c.fn(df))
        return out
    return Col(run, f"coalesce({','.join(c.name for c in cols)})")


def sum(c):            return Agg("sum", _c(c), f"sum({_n(c)})")
def count(c):          return Agg("count", None if _n(c) == "1" else _c(c), f"count({_n(c)})")
def countDistinct(c):  return Agg("countDistinct", _c(c), f"count(distinct {_n(c)})")


def _c(c): return col(c) if isinstance(c, str) else c
def _n(c): return c if isinstance(c, str) else c.name


class F:
    col, lit, when, upper, trim, coalesce = (
        staticmethod(col), staticmethod(lit), staticmethod(when),
        staticmethod(upper), staticmethod(trim), staticmethod(coalesce))
    sum, count, countDistinct = (
        staticmethod(sum), staticmethod(count), staticmethod(countDistinct))


# ---------------------------------------------------------------------------
# DataFrame
# ---------------------------------------------------------------------------
class Row(dict):
    def __getitem__(self, k):
        return super().__getitem__(k)


class DataFrame:
    def __init__(self, pdf):
        self._pdf = pdf.reset_index(drop=True)

    @property
    def columns(self):
        return list(self._pdf.columns)

    def filter(self, cond):
        mask = cond.fn(self._pdf).fillna(False)
        return DataFrame(self._pdf[mask])

    where = filter

    def withColumn(self, name, c):
        pdf = self._pdf.copy()
        pdf[name] = _as_series(c, pdf)
        return DataFrame(pdf)

    def withColumnRenamed(self, old, new):
        # Spark silently ignores a rename of a column that does not exist. Preserved,
        # because the notebooks now guard against exactly this behaviour.
        if old not in self._pdf.columns:
            return self
        return DataFrame(self._pdf.rename(columns={old: new}))

    def select(self, *cols):
        names = []
        pdf = self._pdf.copy()
        for c in cols:
            if isinstance(c, str):
                if c not in pdf.columns:
                    raise KeyError(f"column not found in select: {c}")
                names.append(c)
            else:
                pdf[c.name] = c.fn(pdf)
                names.append(c.name)
        return DataFrame(pdf[names])

    def distinct(self):
        return DataFrame(self._pdf.drop_duplicates())

    def groupBy(self, *keys):
        return _Grouped(self._pdf, [k if isinstance(k, str) else k.name for k in keys])

    def agg(self, *aggs):
        row = {a.name: a.apply(self._pdf) for a in aggs}
        return DataFrame(pd.DataFrame([row]))

    def count(self):
        return len(self._pdf)

    def collect(self):
        return [Row(r) for r in self._pdf.to_dict("records")]

    def cache(self):     return self
    def display(self):   pass
    def show(self, *a):  pass
    def toPandas(self):  return self._pdf.copy()


class _Grouped:
    def __init__(self, pdf, keys):
        self.pdf, self.keys = pdf, keys

    def agg(self, *aggs):
        rows = []
        missing = [k for k in self.keys if k not in self.pdf.columns]
        if missing:
            raise KeyError(f"group key not found: {missing}")
        if len(self.pdf) == 0:
            return DataFrame(pd.DataFrame(columns=self.keys + [a.name for a in aggs]))
        for vals, grp in self.pdf.groupby(self.keys, dropna=False):
            vals = vals if isinstance(vals, tuple) else (vals,)
            row = dict(zip(self.keys, vals))
            for a in aggs:
                row[a.name] = a.apply(grp)
            rows.append(row)
        return DataFrame(pd.DataFrame(rows))

    def count(self):
        out = self.pdf.groupby(self.keys, dropna=False).size().reset_index(name="count")
        return DataFrame(out)


class AnalysisException(Exception):
    pass


class FakeSpark:
    """Catalog stub. Register tables with .register(name, pandas_df)."""

    def __init__(self):
        self.tables = {}

    def register(self, name, pdf):
        self.tables[name] = pdf

    def table(self, name):
        if name not in self.tables:
            raise AnalysisException(f"Table or view not found: {name}")
        return DataFrame(self.tables[name])

    def sql(self, q):
        m = re.search(r"describe table\s+(\S+)", q, re.I)
        if m:
            if m.group(1) not in self.tables:
                raise AnalysisException(f"Table or view not found: {m.group(1)}")
            return DataFrame(pd.DataFrame([{"col_name": c} for c in self.tables[m.group(1)].columns]))
        m = re.search(r"from\s+([\w.]+)", q, re.I)
        if m:
            return self.table(m.group(1))
        raise AnalysisException(f"unsupported sql: {q}")


class FakeWidgets:
    def __init__(self, values):
        self.values = values

    def get(self, k):
        return self.values[k]

    def dropdown(self, *a, **k):
        pass


class FakeDbutils:
    def __init__(self, values):
        self.widgets = FakeWidgets(values)


def install():
    """Register fake `pyspark.*` modules so the notebooks' own import lines resolve.

    The notebooks do `from pyspark.sql import functions as F`, so leaving F in the exec
    globals is not enough — the import would shadow it (or fail). This puts the shim
    behind the real import path instead.
    """
    import sys
    import types

    functions = types.ModuleType("pyspark.sql.functions")
    for n in ("col", "lit", "when", "upper", "trim", "coalesce", "sum", "count",
              "countDistinct"):
        setattr(functions, n, globals()[n])

    sql = types.ModuleType("pyspark.sql")
    sql.functions = functions
    sql.DataFrame = DataFrame
    sql.SparkSession = object
    sql.Column = Col

    utils = types.ModuleType("pyspark.sql.utils")
    utils.AnalysisException = AnalysisException

    window = types.ModuleType("pyspark.sql.window")
    window.Window = object

    types_mod = types.ModuleType("pyspark.sql.types")
    for n in ("StringType", "IntegerType", "DoubleType", "NumericType"):
        setattr(types_mod, n, type(n, (), {}))

    root = types.ModuleType("pyspark")
    root.sql = sql

    sys.modules.update({
        "pyspark": root,
        "pyspark.sql": sql,
        "pyspark.sql.functions": functions,
        "pyspark.sql.utils": utils,
        "pyspark.sql.window": window,
        "pyspark.sql.types": types_mod,
    })
