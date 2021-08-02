# break-up-pipeline
* To compile our codes, you need to first get the source codes of PostgreSQL 12 on Windows. And then placing our given .c file into corresponding file folder. All needed file folders are given by Postgres.
* Adding the given files to the postgres project and compiling the postgres project by VS studio.
* At last, you should replace the original postgres.exe by your new-compiled postgres.exe.

# optimal plan optimizer
## Experiment1
* Using the benchmark in Agg filefolder, and add "explain (analyze)" on the head of queries. And the source codes of experiment are in Experiment1 filefolder.

# Strategy 1, 2 and 3
## Experiment2
* Using the benchmark in NoAgg filefolder, because we add aggregate function in the source code. And the source codes of experiment are in Experiment2 filefolder.
