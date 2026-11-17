"""
Local dry-run of the Olist data-quality rules, WITHOUT Databricks.

Re-implements the logic of notebooks 00 / 04 with pandas + SQLite so you can see, on the real CSVs, which rules fire and
how many records would be quarantined, and so the governed KPIs can be reconciled with the earlier Case Study 4 dashboard.

  python tools/local_dry_run.py "<folder with the 9 Olist CSVs>"  [--report docs/LOCAL_DRY_RUN.md]

It reads the rule list from tools/build_rules.py (single source of truth) and the typed-staging contract from notebooks/00_setup.py.
SQLite is used only to evaluate each rule's pass-condition; a few Spark spellings are translated (RLIKE, greatest, TIMESTAMP literal).
This is a faithful *re-implementation*, not Spark: treat it as a rehearsal, and rely on Databricks for the real run.
"""
import argparse, importlib.util, pathlib, re, sqlite3, sys
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent.parent

# ------------------------------------------------------------------ load shared config
spec = importlib.util.spec_from_file_location("build_rules", ROOT / "tools" / "build_rules.py")
br = importlib.util.module_from_spec(spec); spec.loader.exec_module(br)          # also (re)generates docs/DQ_RULES.md, harmless
RULES = br.RULES

setup_src = (ROOT / "notebooks" / "00_setup.py").read_text()
cfg = setup_src[setup_src.index("# source file -> raw table name"): setup_src.index('print(f"catalog=')]
ns = {}; exec(cfg, ns)
FILES, STAGING, KEYS, PROCESS_ORDER = ns["FILES"], ns["STAGING"], ns["KEYS"], ns["PROCESS_ORDER"]

TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$")

# ------------------------------------------------------------------ staging (mirrors 04_dq_engine.stage)
def clean(s):
    s = s.astype("string").str.strip()
    return s.where(s != "", None)

def stage(raw: pd.DataFrame, table: str) -> pd.DataFrame:
    out = pd.DataFrame({"_row_id": range(len(raw))})
    for c, typ in STAGING[table].items():
        s = clean(raw[c])
        if typ == "string":
            out[c] = s.astype(object)
            continue
        out[c + "__raw"] = s.astype(object)
        if typ == "int":
            v = pd.to_numeric(s, errors="coerce")
            out[c] = v.astype("Float64").round(0).astype(object).where(v.notna(), None)   # whole numbers; fractional parts truncated like the notebook
        elif typ in ("double",) or typ.startswith("decimal"):
            out[c] = pd.to_numeric(s, errors="coerce").astype(object).where(pd.to_numeric(s, errors="coerce").notna(), None)
        elif typ == "timestamp":
            ok = s.fillna("").map(lambda x: bool(TS_RE.match(x)))
            out[c] = s.where(ok, None).str.replace("T", " ", regex=False).astype(object)
    return out

def add_dup_rank(df, keys):
    present = df[keys].notna().all(axis=1)
    rank = df.groupby(keys, dropna=False).cumcount() + 1
    df["_dup_rank"] = rank.where(present, 1)
    return df

# ------------------------------------------------------------------ evaluate rules with SQLite
def to_sqlite_expr(e):
    e = re.sub(r"\bRLIKE\b", "REGEXP", e)
    e = re.sub(r"\bgreatest\(", "max(", e)
    e = re.sub(r"TIMESTAMP'([^']*)'", r"'\1'", e)
    e = e.replace("current_timestamp()", "CURRENT_TIMESTAMP")
    return e

con = sqlite3.connect(":memory:")
con.create_function("REGEXP", 2, lambda pat, s: None if s is None else int(re.search(pat, s) is not None))

def evaluate(table, df, helpers):
    rules = [r for r in RULES if r["table_name"] == table]
    df = add_dup_rank(df, KEYS[table] if table != "geolocation" else ["_row_id"])
    for k, v in helpers.items():
        df[k] = v
    sql_df = df.copy()
    for c in sql_df.columns:                      # SQLite has no pandas extension types
        if sql_df[c].dtype == "boolean" or str(sql_df[c].dtype) == "bool":
            sql_df[c] = sql_df[c].astype(int)
    sql_df.to_sql("t", con, if_exists="replace", index=False)
    flags = pd.DataFrame(index=df.index)
    for r in rules:
        q = f"SELECT CASE WHEN COALESCE(({to_sqlite_expr(r['pass_expr'])}), 0) THEN 0 ELSE 1 END AS f FROM t ORDER BY rowid"
        flags[r["rule_id"]] = pd.read_sql(q, con)["f"].astype(bool).values
    qids = [r["rule_id"] for r in rules if r["failure_action"] == "QUARANTINE"]
    wids = [r["rule_id"] for r in rules if r["failure_action"] == "FLAG"]
    quarantined = flags[qids].any(axis=1) if qids else pd.Series(False, index=df.index)
    flagged = flags[wids].any(axis=1) if wids else pd.Series(False, index=df.index)
    return df, flags, quarantined, flagged, rules

# ------------------------------------------------------------------ KPIs (same definitions as semantic layer / earlier project)
def kpis(orders, customers, items, pays, reviews):
    o = orders.merge(customers[["customer_id", "customer_unique_id", "customer_state"]], on="customer_id", how="inner")
    it = items.groupby("order_id").agg(items_count=("price", "size"), items_value=("price", "sum"), freight=("freight_value", "sum")).reset_index()
    o = o.merge(it, on="order_id", how="left")
    for c in ("items_value", "freight"):
        o[c] = o[c].fillna(0.0).astype(float)
    o["order_value"] = o["items_value"] + o["freight"]
    o["is_valid"] = ~o["order_status"].isin(["canceled", "unavailable"])
    ts = lambda c: pd.to_datetime(o[c], errors="coerce")
    deliv = ts("order_delivered_customer_date").where(o["order_status"] == "delivered")
    o["delivered_date"] = deliv.dt.normalize()
    o["estimated_date"] = ts("order_estimated_delivery_date").dt.normalize()
    o["purchase_date"] = ts("order_purchase_timestamp").dt.normalize()
    o["has_delivery"] = o["delivered_date"].notna()
    o["delivery_days"] = (o["delivered_date"] - o["purchase_date"]).dt.days
    o["is_late"] = (o["delivered_date"] > o["estimated_date"]) & o["has_delivery"]
    r = reviews.copy()
    r["_ans"] = pd.to_datetime(r["review_answer_timestamp"], errors="coerce")
    r = r.sort_values(["order_id", "_ans", "review_id"], ascending=[True, False, True], na_position="last").drop_duplicates("order_id")
    o = o.merge(r[["order_id", "review_score"]], on="order_id", how="left")
    v = o[o["is_valid"]]
    return {
        "Orders (all)": len(o), "Order items": len(items), "Sum of item prices, all orders (BRL)": float(items["price"].sum()),
        "Sum of payments (BRL)": float(pays["payment_value"].sum()), "Valid orders": len(v),
        "Unique customers (valid orders)": v["customer_unique_id"].nunique(), "Sales (Items), valid orders (BRL)": float(v["items_value"].sum()),
        "AOV (BRL)": float(v["order_value"].sum() / len(v)), "Delivered orders with delivered date": int(o["has_delivery"].sum()),
        "Late delivery %": float(100.0 * o["is_late"].sum() / o["has_delivery"].sum()), "Avg delivery days": float(o["delivery_days"].mean()),
        "Reviewed orders": int(o["review_score"].notna().sum()), "Avg review score": float(o["review_score"].mean()),
    }

BASELINE = {  # published in the earlier Case Study 4 project
    "Orders (all)": 99441, "Order items": 112650, "Sum of item prices, all orders (BRL)": 13591643.70, "Sum of payments (BRL)": 16008872.12,
    "Valid orders": 98207, "Unique customers (valid orders)": 94990, "Sales (Items), valid orders (BRL)": 13494400.74, "AOV (BRL)": 160.23,
    "Delivered orders with delivered date": 96470, "Late delivery %": 6.77, "Avg delivery days": 12.5, "Reviewed orders": 98673, "Avg review score": 4.09,
}

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("data_dir"); ap.add_argument("--report", default=str(ROOT / "docs" / "LOCAL_DRY_RUN.md"))
    a = ap.parse_args()
    d = pathlib.Path(a.data_dir)
    raw = {t: pd.read_csv(d / f, dtype=str, keep_default_na=False, encoding="utf-8") for t, f in FILES.items()}
    stg = {t: stage(raw[t], t) for t in FILES}
    valid, rule_rows, summ, qsamples = {}, [], [], []

    def run(table, helpers=None):
        df, flags, quar, flagd, rules = evaluate(table, stg[table].copy(), helpers or {})
        valid[table] = df[~quar].reset_index(drop=True)
        n = len(df)
        summ.append((table, n, int((~quar).sum()), int(quar.sum()), int((flagd & ~quar).sum())))
        for r in rules:
            rule_rows.append((r["rule_id"], r["dimension"], table, r["failure_action"], n, int(flags[r["rule_id"]].sum()), r["rule_description"], r["target_pass_rate"]))
        bad = df[quar]
        if len(bad):
            top = flags[quar].apply(lambda row: ",".join(c for c in flags.columns if row[c] and c in [x["rule_id"] for x in rules if x["failure_action"] == "QUARANTINE"]), axis=1)
            for i in bad.index[:3]:
                qsamples.append((table, ", ".join(str(bad.loc[i, k]) for k in KEYS[table] if k in bad.columns), top.loc[i]))

    clean_str = lambda s: s.astype("string").str.strip().where(lambda x: x != "", None)
    run("category_translation"); run("geolocation"); run("customers", {"_zip_in_geo": stg["customers"]["customer_zip_code_prefix"].isin(set(valid["geolocation"]["geolocation_zip_code_prefix"].dropna())).astype(int)})
    run("sellers"); run("products", {"_cat_in_translation": stg["products"]["product_category_name"].isin(set(valid["category_translation"]["product_category_name"].dropna())).astype(int)})
    # orders helpers
    ri = stg["order_items"]; rp = stg["payments"]
    items_tot = (ri["price"].astype(float) + ri["freight_value"].astype(float)).groupby(ri["order_id"]).sum(min_count=1)
    pay_tot = rp["payment_value"].astype(float).groupby(rp["order_id"]).sum(min_count=1)
    so = stg["orders"]
    run("orders", {"_customer_ok": so["customer_id"].isin(set(valid["customers"]["customer_id"])).astype(int),
                   "_has_item_raw": so["order_id"].isin(set(ri["order_id"].dropna())).astype(int),
                   "_items_total": so["order_id"].map(items_tot), "_pay_total": so["order_id"].map(pay_tot)})
    vo = valid["orders"].set_index("order_id")["order_purchase_timestamp"]
    run("order_items", {"_order_ok": ri["order_id"].isin(set(vo.index)).astype(int), "_product_ok": ri["product_id"].isin(set(valid["products"]["product_id"])).astype(int),
                        "_seller_ok": ri["seller_id"].isin(set(valid["sellers"]["seller_id"])).astype(int), "_order_purchase_ts": ri["order_id"].map(vo)})
    run("payments", {"_order_ok": rp["order_id"].isin(set(vo.index)).astype(int)})
    rr = stg["reviews"]
    run("reviews", {"_order_ok": rr["order_id"].isin(set(vo.index)).astype(int), "_reviews_per_order": rr.groupby("order_id")["review_id"].transform("size")})

    # ------------------------------------------------------------------ report
    S = pd.DataFrame(summ, columns=["table", "evaluated", "valid", "quarantined", "valid_with_warnings"])
    S["pass_rate_%"] = (100 * S["valid"] / S["evaluated"]).round(3)
    Rr = pd.DataFrame(rule_rows, columns=["rule_id", "dimension", "table", "action", "evaluated", "failed", "rule", "target_%"])
    Rr["pass_rate_%"] = (100 * (Rr["evaluated"] - Rr["failed"]) / Rr["evaluated"]).round(3)
    Rr["status"] = Rr.apply(lambda x: "PASSED" if x["pass_rate_%"] >= x["target_%"] else ("WARNING" if x["action"] == "FLAG" else "FAILED"), axis=1)

    raw_typed = {t: stg[t] for t in FILES}
    k_raw = kpis(*[raw_typed[t].assign(**({} if True else {})) for t in ("orders",)], raw_typed["customers"], raw_typed["order_items"], raw_typed["payments"], raw_typed["reviews"])
    k_gov = kpis(valid["orders"], valid["customers"], valid["order_items"], valid["payments"], valid["reviews"])
    K = pd.DataFrame([(m, BASELINE[m], k_raw[m], k_gov[m]) for m in BASELINE], columns=["metric", "earlier_dashboard", "recomputed_from_raw_(no_DQ)", "governed_(valid_only)"])
    K["raw_vs_earlier_%"] = (100 * (K.iloc[:, 2] - K.iloc[:, 1]) / K.iloc[:, 1]).round(4)
    K["governed_vs_earlier_%"] = (100 * (K.iloc[:, 3] - K.iloc[:, 1]) / K.iloc[:, 1]).round(4)

    pd.set_option("display.width", 250, "display.max_columns", 30, "display.max_colwidth", 70)
    print("\n=== PER TABLE ===\n", S.to_string(index=False))
    print("\n=== RULES THAT FIRED (failed > 0) ===\n", Rr[Rr.failed > 0].sort_values("failed", ascending=False)[["rule_id", "dimension", "table", "action", "failed", "pass_rate_%", "target_%", "status", "rule"]].to_string(index=False))
    print("\n=== RULES BELOW TARGET ===\n", Rr[Rr.status != "PASSED"][["rule_id", "table", "action", "failed", "pass_rate_%", "target_%", "status"]].to_string(index=False))
    print("\n=== KPIs ===\n", K.to_string(index=False))
    tot = S[["evaluated", "valid", "quarantined"]].sum()
    print(f"\nTOTAL evaluated={tot.evaluated:,} valid={tot.valid:,} quarantined={tot.quarantined:,} pass rate={100*tot.valid/tot.evaluated:.3f}%")

    # markdown report
    md = ["# Local dry-run results", "", "Produced by `tools/local_dry_run.py` (pandas + SQLite re-implementation of the engine; not Spark).", "",
          f"**Total:** {tot.evaluated:,} records evaluated, {tot.valid:,} valid, {tot.quarantined:,} quarantined (pass rate {100*tot.valid/tot.evaluated:.3f} %).", "",
          "## Per table", "", S.to_markdown(index=False), "", "## Rules that fired", "",
          Rr[Rr.failed > 0].sort_values("failed", ascending=False)[["rule_id", "dimension", "table", "action", "failed", "pass_rate_%", "target_%", "status", "rule"]].to_markdown(index=False), "",
          "## KPI reconciliation", "", K.to_markdown(index=False), ""]
    pathlib.Path(a.report).write_text("\n".join(md))
    print("report ->", a.report)

if __name__ == "__main__":
    main()
