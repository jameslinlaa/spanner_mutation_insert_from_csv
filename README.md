# Isert data into Spanner from a csv file, and record the execution time
This is a sample test tool to evaluate insert/delete execution time (End to End). \
Read a bounch of records from csv files, then insert them into Spanner table. \
Run the program from a GCE VM or on-prem VM to get the E2E execution time. 

# Some Notes
1. Insert records via mutation apis (instead of DML)
2. Delete the records via partitioned dml
3. In this sample, we just use single thread to insert the records. \
    The performance can be much improved if via multi-threads.  