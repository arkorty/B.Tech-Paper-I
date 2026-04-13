import csv
from collections import defaultdict
import statistics

data = defaultdict(list)
with open("full_analysis_1.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cond = row["condition"]
        data[cond].append(row)

for cond in ["no_guard", "with_guard"]:
    rows = data[cond]
    print(f"\nCondition: {cond} (N={len(rows)})")
    agreements = [int(r["agreement"]) for r in rows if r["agreement"].strip() != ""]
    print(f"Agreement Rate: {sum(agreements) / len(agreements) * 100:.1f}%")
    
    rounds = [int(r["rounds"]) for r in rows if r["rounds"].strip() != ""]
    print(f"Avg Rounds: {statistics.mean(rounds):.2f}, Variance: {statistics.variance(rounds):.2f}")
    
    deviations = [float(r["midpoint_deviation_usd"]) for r in rows if r["midpoint_deviation_usd"].strip() != ""]
    print(f"Avg Dev: {statistics.mean(deviations):.2f}, Max Dev (Worst): {max(deviations):.2f}")
    print(f"Variance of Dev: {statistics.variance(deviations):.2f}")
    
    violations = [int(float(r["final_violation"])) for r in rows if r["final_violation"].strip() != ""]
    print(f"Final Violation Rate: {sum(violations) / len(violations) * 100:.1f}%")

    unsafe = [int(r["unsafe_attempts"]) for r in rows if r["unsafe_attempts"].strip() != ""]
    print(f"Avg Unsafe Attempts / Run: {statistics.mean(unsafe):.2f}")
