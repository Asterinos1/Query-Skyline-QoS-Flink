import pandas as pd
import matplotlib.pyplot as plt

# 1. SETUP: Map the filenames to the corresponding plots
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
fig, axes = plt.subplots(1, 3, figsize=(18, 5))
titles = ['Performance in 2 Dimensions', 'Performance in 3 Dimensions', 'Performance in 4 Dimensions']
file_groups = [files_2d, files_3d, files_4d]

# Styling to match the examples
markers = {'MR-Angle': 'o', 'MR-Dim': 's', 'MR-Grid': '^'}
colors = {'MR-Angle': 'royalblue', 'MR-Dim': 'peru', 'MR-Grid': 'seagreen'}

# 3. GENERATE PLOTS
for i, (ax, files) in enumerate(zip(axes, file_groups)):
    for algo, filepath in files.items():
        try:
            # Read the CSV
            df = pd.read_csv(filepath)
            
            # Data Transformation
            # Convert Records to 'Millions' for the X-axis
            x = df['Records'] / 1_000_000 
            
            # Convert TotalTime(ms) to 'Seconds' for the Y-axis
            y = df['TotalTime(ms)'] / 1000 
            
            # Plot the line
            ax.plot(x, y, marker=markers[algo], label=algo, color=colors[algo])
        except FileNotFoundError:
            print(f"Warning: File {filepath} not found. Skipping.")

    # Chart Styling
    ax.set_title(titles[i])
    ax.set_xlabel('Records (Millions)')
    ax.set_ylabel('Total Time (Seconds)')
    ax.grid(True, alpha=0.3)
    ax.legend()

plt.tight_layout()
plt.savefig('performance_plots.png')
plt.show()