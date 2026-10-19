"""Render e2e results as Markdown for the GitHub Actions job summary."""
import json
import os

out = ["## End-to-end results"]
for f in ("e2e_first.json", "e2e_replay.json", "e2e_maintenance.json"):
    if not os.path.exists(f):
        continue
    r = json.load(open(f))
    out.append(f"### {r['label']}: {'PASSED' if r['passed'] else 'FAILED'}")
    out.append("| check | result |\n|---|---|")
    out += [f"| {k} | {'pass' if v else 'FAIL'} |" for k, v in r["checks"].items()]
    e, g = r["expected"], r["got"]
    out.append(f"\nsent {e['sent']} records ({e['valid_unique']} unique valid, {e['duplicates']} duplicates, "
               f"{e['late']} late, {e['bad']} bad) -> bronze {g['bronze_rows']}, silver {g['silver_rows']}, "
               f"quarantine {g['reject_rows']}, gold samples {g['gold_samples_total']}\n")
if os.path.exists("dq_report.json"):
    out += ["### Data-quality gate", "```json", open("dq_report.json").read(), "```"]
print("\n".join(out))
