import csv
import os
import re
import sys

# map directory tokens to column values
PROMPT_MAP = {"short": "1", "medium": "2", "long": "3"}
TQ_MAP = {"TRUE": "turboquant", "FALSE": "vanilla"}

# regex to parse directory names like:
# Meta-Llama-3.1-8B-Instruct-Q4_K_M__n1__t46__tr3__mt1024__tqFALSE__short__c4
# Note: concurrency part (__cXX) is optional
DIR_RE = re.compile(
    r"^(?P<model>.+?)__n(?P<n>\d+)__t(?P<t>\d+)__tr(?P<tr>\d+)__mt(?P<mt>\d+)__tq(?P<tq>TRUE|FALSE)__(?P<prompt>short|medium|long)(?:__c(?P<concurrency>\d+))?$"
)


def parse_dirname(name):
    m = DIR_RE.match(name)
    if not m:
        return None
    data = {
        "model": m.group("model"),
        "n": m.group("n"),
        "t": m.group("t"),
        "run_group": TQ_MAP[m.group("tq")],
        "prompt_id": PROMPT_MAP[m.group("prompt")],
    }
    # concurrency may be missing (older runs)
    if m.group("concurrency"):
        data["concurrency"] = m.group("concurrency")
    else:
        data["concurrency"] = "1"  # assume default concurrency 1 if not present
    return data


def main(root_dir, output_file):
    all_rows = []
    fieldnames = None

    for entry in sorted(os.scandir(root_dir), key=lambda e: e.name):
        if not entry.is_dir():
            continue

        meta = parse_dirname(entry.name)
        if meta is None:
            print(f"skipping (no match): {entry.name}", file=sys.stderr)
            continue

        results_path = os.path.join(entry.path, "results.csv")
        if not os.path.exists(results_path):
            print(f"missing results.csv: {entry.name}", file=sys.stderr)
            continue

        with open(results_path, newline="") as f:
            reader = csv.DictReader(f)

            if fieldnames is None:
                # metadata columns to prepend (if not already in CSV)
                meta_cols = [
                    k
                    for k in [
                        "run_group",
                        "model",
                        "n",
                        "t",
                        "prompt_id",
                        "concurrency",
                    ]
                    if k not in reader.fieldnames
                ]
                fieldnames = meta_cols + reader.fieldnames

            for row in reader:
                # inject metadata columns not already present
                for k, v in meta.items():
                    if k not in row:
                        row[k] = v
                all_rows.append(row)

    if not all_rows:
        print("no rows found — check root_dir path", file=sys.stderr)
        sys.exit(1)

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"wrote {len(all_rows)} rows to {output_file}")


if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    out = sys.argv[2] if len(sys.argv) > 2 else "combined.csv"
    main(root, out)
