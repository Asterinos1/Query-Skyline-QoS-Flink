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
    "MR-Dim":   [65, 140, 420],  # Times for Dim 2, 4, 8
    "MR-Grid":  [50, 100, 320],
    "MR-Angle": [45, 60,  130]
}

# Objective 3: Local Optimality (0.0 - 1.0)
# Example Data (Replace with yours!)
optimality_results = {
    "MR-Dim":   [0.1, 0.2, 0.3],
    "MR-Grid":  [0.15, 0.25, 0.4],
    "MR-Angle": [0.25, 0.5, 0.8] 
}
# ==========================================

def plot_paper_figures():
    # --- Figure 5 Replication (Time vs Dim) ---
    plt.figure(figsize=(10, 5))
    for algo, times in time_results.items():
        plt.plot(dimensions, times, marker='o', label=algo)
    
    plt.title("Processing Time vs Dimensionality (Paper Fig 5)")
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

    plt.title("Local Skyline Optimality vs Dimensionality (Paper Fig 7)")
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