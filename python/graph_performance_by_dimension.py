import pandas as pd
import matplotlib.pyplot as plt

"""
Performance Visualization Script.

This script generates a comparative performance analysis of three distributed Skyline algorithms
(MR-Angle, MR-Dim, MR-Grid) across different dimensionalities (2D, 3D, and 4D).

It operates by reading a set of CSV output files—produced by the Flink job—and rendering
their performance metrics onto three side-by-side line charts. The script handles data 
transformation automatically, converting raw record counts into millions and processing 
timestamps from milliseconds into seconds for better readability.

The final output is a single image file ('performance_plots.png') containing the three 
subplots, which is suitable for inclusion in academic papers or technical reports.
"""

# 1. SETUP: File Mapping
# We define dictionaries to map the specific algorithm names to their corresponding 
# CSV result files for each dimension. This allows the script to iterate through 
# the data in a structured manner without hardcoding file paths inside the loop.

# Plot 1: 2 Dimensions
files_2d = {
    'MR-Angle': 'mrAngle_2dims.csv',
    'MR-Dim': 'mrDim_2dims.csv',
    'MR-Grid': 'mrGrid_2dims.csv'
}

# Plot 2: 3 Dimensions
files_3d = {
    'MR-Angle': 'mrAngle_3dims.csv',
    'MR-Dim': 'mrDim_3dims.csv',
    'MR-Grid': 'mrGrid_3dims.csv'
}

# Plot 3: 4 Dimensions
files_4d = {
    'MR-Angle': 'mrAngle_4dims_50000.csv',
    'MR-Dim': 'mrDim_4dims_50000.csv',
    'MR-Grid': 'mrGrid_4dims_50000.csv'
}

# 2. PLOTTING CONFIGURATION
# We initialize a matplotlib figure with three subplots arranged horizontally.
# The figure size is set to be wide (18x5 inches) to accommodate the three charts 
# comfortably without cramping the labels.
fig, axes = plt.subplots(1, 3, figsize=(18, 5))
titles = ['Performance in 2 Dimensions', 'Performance in 3 Dimensions', 'Performance in 4 Dimensions']
file_groups = [files_2d, files_3d, files_4d]

# Styling Constants
# These dictionaries enforce consistent visual identity across all plots.
# We assign specific markers (Circle, Square, Triangle) and colors to each algorithm 
# to make them easily distinguishable even in black-and-white print.
markers = {'MR-Angle': 'o', 'MR-Dim': 's', 'MR-Grid': '^'}
colors = {'MR-Angle': 'royalblue', 'MR-Dim': 'peru', 'MR-Grid': 'seagreen'}

# 3. GENERATE PLOTS
# The outer loop iterates through the three subplots (axes) and their corresponding 
# file groups (2D, 3D, 4D). The inner loop handles the actual data loading and plotting 
# for each algorithm within that dimension.
for i, (ax, files) in enumerate(zip(axes, file_groups)):
    for algo, filepath in files.items():
        try:
            # Read the CSV Data
            df = pd.read_csv(filepath)
            
            # Data Transformation
            # The X-axis raw data is in single units. We divide by one million to 
            # make the axis labels cleaner (e.g., "1" instead of "1000000").
            x = df['Records'] / 1_000_000 
            
            # The Y-axis raw data is in milliseconds. We divide by 1000 to convert 
            # this to seconds, which is a more intuitive unit for these durations.
            y = df['TotalTime(ms)'] / 1000 
            
            # Plot the line for the current algorithm
            ax.plot(x, y, marker=markers[algo], label=algo, color=colors[algo])
        except FileNotFoundError:
            # Graceful error handling allows the script to continue generating partial 
            # graphs even if one CSV file is missing.
            print(f"Warning: File {filepath} not found. Skipping.")

    # Chart Styling
    # We apply standard labeling and grid lines to the current subplot to ensure 
    # it is readable and follows scientific plotting conventions.
    ax.set_title(titles[i])
    ax.set_xlabel('Records (Millions)')
    ax.set_ylabel('Total Time (Seconds)')
    ax.grid(True, alpha=0.3)
    ax.legend()

# Layout Adjustment and Saving
# 'tight_layout' automatically adjusts subplot params so that subplots are nicely 
# fit in to the figure area. We then save the result to disk and display it.
plt.tight_layout()
plt.savefig('performance_plots.png')
plt.show()
