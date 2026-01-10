import matplotlib.pyplot as plt
import numpy as np

# ==========================================
# ENTER YOUR RESULTS HERE
# ==========================================
# Run your experiments and copy the "TotalTime(ms)" 
# for 100,000 records (Small) or 10M records (Large)

dimensions = [2, 4, 8]

# Objective 2: Processing Time (Seconds)
# Example Data (Replace with yours!)
time_results = {
    "MR-Dim":   [19544, 27264, 716996],  # Times for Dim 2, 4, 8
    "MR-Grid":  [17593, 26601, 691882],
    "MR-Angle": [17282, 27015,  766937]
}

# Objective 3: Local Optimality (0.0 - 1.0)
# Example Data (Replace with yours!)
optimality_results = {
    "MR-Dim":   [0.7379, 0.6742, 0.25],
    "MR-Grid":  [0.5415, 0.5906, 0.25],
    "MR-Angle": [0.7453, 0.6652, 0.25] 
}
# ==========================================

def plot_paper_figures():
    # --- Figure 5 Replication (Time vs Dim) ---
    plt.figure(figsize=(10, 5))
    for algo, times in time_results.items():
        plt.plot(dimensions, times, marker='o', label=algo)
    
    plt.title("Processing Time vs Dimensionality (Cardinality 1 Million)")
    plt.xlabel("Dimensions")
    plt.ylabel("Processing Time (s)")
    plt.xticks(dimensions)
    plt.legend()
    plt.grid(True)
    plt.savefig("figure_5_replication.png")
    print("Saved figure_5_replication.png")

    # --- Figure 7 Replication (Optimality vs Dim) ---
    plt.figure(figsize=(10, 5))
    for algo, opts in optimality_results.items():
        plt.plot(dimensions, opts, marker='s', linestyle='--', label=algo)

    plt.title("Local Skyline Optimality vs Dimensionality (Cardinality 1 Million)")
    plt.xlabel("Dimensions")
    plt.ylabel("Optimality Ratio")
    plt.xticks(dimensions)
    plt.legend()
    plt.grid(True)
    plt.savefig("figure_7_replication.png")
    print("Saved figure_7_replication.png")
    
    plt.show()

if __name__ == "__main__":
    plot_paper_figures()
