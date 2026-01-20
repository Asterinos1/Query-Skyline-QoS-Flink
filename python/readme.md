## Project Scripts

This project includes a suite of 8 Python scripts for generating data, collecting metrics from the Flink job, and visualizing the results.

* **unified_producer.py**: The main script to run experiments. It continuously generates data and automatically sends query triggers to a control topic based on the ***QUERY_THRESHOLD*** constant defined in the script.
    ```bash
    # Usage: python unified_producer.py <data_topic> <distribution> <dimensions> [min] [max] [query_topic]
    python python/unified_producer.py input-tuples correlated 3 0 1000 queries
    ```

* **kafka_producer.py**: A simpler version of the producer that only generates data without sending any query triggers. Use this to test maximum ingestion speed.
    ```bash
    # Usage: python kafka_producer.py <topic_name> <distribution> <dimensions>
    python kafka_producer.py input-data uniform 2
    ```

* **query_trigger.py**: Used to manually send a "Query Trigger" signal to Kafka, forcing the system to calculate the skyline immediately.
    ```bash
    # Usage: python query_trigger.py <topic_name> <algorithm> <interval>
    python query_trigger.py queries mr-angle 60
    ```

* **metrics_collector.py**: Listens to the output Kafka topic and saves the results (latency, processing time, etc.) into a CSV file.
    ```bash
    # Usage: python metrics_collector.py <output_filename.csv>
    python metrics_collector.py results.csv
    ```

* **graph_ingestion_parallelism.py**: Creates a dashboard with 4 charts showing ingestion time, total processing time, optimality, and a breakdown of local vs. global time.
    ```bash
    # Usage: python graph_ingestion_parallelism.py Label1=file1.csv Label2=file2.csv
    python graph_ingestion_parallelism.py MR-Angle=results_angle.csv MR-Grid=results_grid.csv
    ```

* **graph_skyline_points_2d.py**: Visualization for 2D data. It plots the calculated skyline points and draws a step-line to verify the results are correct.
    ```bash
    # Usage: python graph_skyline_points_2d.py <results_file.csv> <row_index>
    python graph_skyline_points_2d.py results.csv 5
    ```

* **graph_performance_by_dimension.py**: Generates comparison charts to see how different algorithms (MR-Angle, MR-Dim, MR-Grid) perform as you increase dimensions (2D, 3D, 4D).
    ```bash
    # Usage: Run directly (requires specific CSV filenames in the directory)
    python graph_performance_by_dimension.py
    ```

* **graph_paper_figures.py**: A script designed to exactly replicate the specific figures (Time vs. Dimension and Optimality vs. Dimension) used in the project report.
    ```bash
    # Usage: Run directly (edit script to input your specific data points)
    python graph_paper_figures.py
    ```
