import pandas as pd
import matplotlib.pyplot as plt
import json
import sys
import numpy as np

def plot_skyline(csv_file, row_index=-1):
    try:
        df = pd.read_csv(csv_file)
        
        # Get the specific row (default: last row)
        row = df.iloc[row_index]
        points_json = row["SkylinePoints"]
        
        # Parse JSON
        skyline_points = json.loads(points_json)
        skyline_data = np.array(skyline_points)

        plt.figure(figsize=(10, 8))
        
        # 1. Plot Skyline Points (Red)
        if len(skyline_data) > 0:
            # Sort by X to draw a connected step-line if desired, 
            # but scatter is safer for general skyline
            plt.scatter(skyline_data[:, 0], skyline_data[:, 1], c='red', marker='o', label='Skyline Points')
            
            # Optional: Draw step-line connecting them (Skyline aesthetic)
            skyline_data = skyline_data[skyline_data[:, 0].argsort()]
            plt.step(skyline_data[:, 0], skyline_data[:, 1], where='post', linestyle='--', color='red', alpha=0.5)

        # 2. Formatting
        plt.title(f"Skyline Visualization (Query {row['QueryID']})")
        plt.xlabel("Dimension 1")
        plt.ylabel("Dimension 2")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.5)
        
        output_file = f"skyline_viz_{row['QueryID']}.png"
        plt.savefig(output_file)
        print(f"Visualization saved to {output_file}")
        plt.show()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python graph_skyline_2d.py <results.csv> [row_index]")
        sys.exit(1)
        
    csv_path = sys.argv[1]
    idx = int(sys.argv[2]) if len(sys.argv) > 2 else -1
    plot_skyline(csv_path, idx)