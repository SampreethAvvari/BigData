# Lab 4: Dask

- Name: Sampreeth avvari, Dhruv Sridhar, Barath Rama Shankar

- NetID: spa9659, ds7395, br2543

## Description of your solution


The objective of this project is to compute the largest temperature range (difference between maximum and minimum temperatures) for each climate measurement station on a daily basis, using the GHCN (Global Historical Climatology Network) dataset. The dataset is large, containing daily summaries of climate data from approximately 118,000 files (37 GB total). This project employs Dask to handle the large data volumes efficiently through parallel processing.

Data Processing Steps
The data processing pipeline can be broken down into the following steps:

Data Filtering: Only records where both TMAX and TMIN measurements are present and have passed quality checks (quality == '  ') are considered. Records with a value of -9999 are discarded as they indicate missing data.
Date Parsing: Each record's date components (year, month, day) are parsed into a proper datetime format to facilitate temporal operations.
Temperature Range Calculation: For each day at each station, the difference between TMAX and TMIN is computed. This involves a fold-by operation that tracks the highest TMAX and lowest TMIN found for each station-date combination, followed by mapping these to calculate the temperature range (t_range).
Aggregation: The daily temperature ranges are then aggregated to find the maximum temperature range recorded for each station across the entire dataset.
Implementation Details
The implementation utilizes Dask's bag and DataFrame structures to handle the dataset's size and complexity:

Dask Bag: Used for initial data loading and filtering, due to its suitability for handling semi-structured data.
Dask DataFrame: Employed post-filtering to leverage optimized group-by and aggregation operations typical of structured data workflows.
Each operation, especially file loading and temperature range calculations, is executed in parallel across multiple batches of data files, allowing for scalable and efficient processing.

Performance Analysis
The computation was run on three different subsets of the data:

Tiny: Contained a very small subset of files for quick testing and debugging.
Small: A larger subset, but manageable enough to allow for rapid iteration.
All: The entire dataset, used for the final computation.

Here are the observed runtimes for each dataset size:

Tiny: Approximately 2 seconds.
Small: Around 20 seconds.
All: Approximately 6 to 9 minutes.
