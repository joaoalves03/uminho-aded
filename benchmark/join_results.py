import csv
import os
import re
import sys
import argparse

# map directory tokens to column values
PROMPT_MAP = {"short": "1", "medium": "2", "long": "3"}
TQ_MAP = {"TRUE": "turboquant", "FALSE": "vanilla"}

# Base regex without concurrency suffix
BASE_RE = r"^(?P<model>.+?)__n(?P<n>\d+)__t(?P<t>\d+)__tr(?P<trials>\d+)__mt(?P<mt>\d+)__tq(?P<tq>TRUE|FALSE)__(?P<prompt>short|medium|long)"
# With optional concurrency suffix
CONC_RE = BASE_RE + r"(?:__c(?P<concurrency>\d+))?$"


def compile_regex(concurrency_flag):
    if concurrency_flag:
        return re.compile(CONC_RE)
    else:
        return re.compile(BASE_RE + r"$")  # strict: no suffix allowed


def parse_dirname(name, regex, concurrency_flag):
    m = regex.match(name)
    if not m:
        return None
    data = {
        "model": m.group("model"),
        "n": m.group("n"),
        "t": m.group("t"),
        "run_group": TQ_MAP[m.group("tq")],
        "prompt_id": PROMPT_MAP[m.group("prompt")],
    }
    # concurrency may be present only if the regex includes that group
    if concurrency_flag:
        # group "concurrency" exists; it may be None if suffix absent
        conc = m.group("concurrency")
        data["concurrency"] = conc if conc is not None else "1"
    else:
        data["concurrency"] = "1"
    return data


def main(root_dir, output_file, concurrency_flag):
    regex = compile_regex(concurrency_flag)
    all_rows = []
    fieldnames = None

    for entry in sorted(os.scandir(root_dir), key=lambda e: e.name):
        if not entry.is_dir():
            continue

        meta = parse_dirname(entry.name, regex, concurrency_flag)
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
                # Ensure peak_memory_mb is in fieldnames
                base_fields = list(reader.fieldnames)
                if "peak_memory_mb" not in base_fields:
                    base_fields.append("peak_memory_mb")
                fieldnames = meta_cols + base_fields

            for row in reader:
                # inject metadata columns not already present
                for k, v in meta.items():
                    if k not in row:
                        row[k] = v
                # Add peak_memory_mb if missing, default "N/A"
                if "peak_memory_mb" not in row:
                    row["peak_memory_mb"] = "N/A"
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
    parser = argparse.ArgumentParser(description="Combine benchmark results CSV files")
    parser.add_argument(
        "root_dir",
        nargs="?",
        default=".",
        help="Root directory containing result subdirectories",
    )
    parser.add_argument(
        "output_file", nargs="?", default="combined.csv", help="Output CSV file name"
    )
    parser.add_argument(
        "--concurrency",
        action="store_true",
        help="Parse directories with concurrency suffix (__cXX)",
    )
    args = parser.parse_args()

    main(args.root_dir, args.output_file, args.concurrency)
