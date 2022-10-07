# Compilation
* In ./src directory, we give all modified files and our new-created files (lfh.h/.c and query_split.h/.c). To compile our codes, you need to first get the source codes of PostgreSQL 12.3. And then placing the given files into corresponding file folder. All needed file folders are given by Postgres.
* Adding the given files to the postgres project or do corresponding modification to makefile, and compiling the postgres project.
* When you install our modifed PostgreSQL, the default query processing mode is orignial PostgreSQL. By embedded command "switch to Postgres;", you can switch the current mode to original PostgreSQL as well.


# Reproduction
* If you want to skip compilation phase, we provide two versions for reproduction.
* The first one is a docker image, which supports query split, optimal optimizer and original PostgreSQL. We compile the source code and place it in the docker image, you can pull the image by command "docker pull zhaojy20/query_split:v1".
* The second one is PostgreSQL UDF "query_split.dll", which is a light-weight reproduction only for query split. We decribe them in detail below repectively.
* However, we do not provide Join Order Benchmark in both versions because of size. To obtain JOB, please search gregrahn/join-order-benchmark in Github and install JOB according to their guidance.

# Details for Docker Image
* To start the image, you can use the command "docker run -it -p 5432:5432 query_split".
* In the image cmd, you can use following command to start the PostgreSQL serever:" su - postgres", "cd /usr/local/pgsql/bin", "./pg_ctl -D ../data -l logfile start". Then, you can start PostgreSQL by "psql -U postgres" and password is 12345. Now, you have a PostgreSQL server listens on 5432 port.
* The image we provide support both query split and optimal optimizer. You can change them by our new-desinged command.
* By "switch to Optimal;", we switch the execution mode to our baseline "optimal optimizer". This mode only support the command decorated by "explain". For example, in this mode, you should iterate the same command "explain(analyze) SELECT ..." until the output plan stops changing. And by "set factor = XX (double);", you can change this factor. If factor < 1, we encourage optimzier to try new join order (we use 0.1 for most queries and 0.5 for some extremely large queries). The less factor is, the longer time is needed to obtain the optimal plan.
* Query split consists of two parts: query splitting algorithm and execution ordr decision. By command "switch to minsubquery;", "switch to relationshipcenter;" and "switch to entitycenter;", you change the query splitting algorithm. By command "switch to oc;"(cost(q)), "switch to or;"(row(q)), "switch to c_r;"(hybrid_row), "switch to c_rsq;"(hybrid_sqrt), "switch to c_rlg;"(hybrid_log) and "switch to global;" (global_sel), you change the execution ordr decision.
* The command "explain" is not supported yet in query split. And query split is not support for non-SPJ query, such as outer join, except, etc.
* By command "enable\disable DM;", you allow or disallow the physical opertor "DirectMap", which is an attempt for the improvement of merge join and hash join.
* Note that When you enter these commands, the database will return "ERROR: syntax error at or near "switch" at character 1", this is OK. Because PostgreSQL parser cannot identify these code correctly, however, the parameters are actually changed by our embedded code.

# Details for query_split.dll
* query_split.dll only supports query split.
* First, we move the query_split.dll to ./lib in the PostgreSQL installation directory.
* Then, we start PostgreSQL and use command "CREATE FUNCTION query_split_exec(cstring, interger, integer) RETURNS void AS 'query_split' LANGUAGE C IMMUTABLE STRICT;".
* After that, use command "select * from query_split_exec('select ...', x, y)" to run the query split, where x is the parameter for query splittiing algorithm (0 for Minsubquery, 1 for RelationshipCenter and 2 for EntityCenter), y is the parameter for execution order decision (0 for cost(q), 1 for row(q), 2 for hybrid_row, 3 for hybrid_sqrt, 4 for hybrid_log, 5 for global_sel). 

# Potential Bugs
* The local buffer would be released after the end of a session. We recommand execute one query each session for reproducing query split, as the local buffer consumpition is very huge in query split. Executing too many queries in one session may lead to some unfixed bugs. In future, we will improve this part.
* Obviously, our work can be further improved. And there would be many potential bugs in our codes. If you meet these bugs, thank you for contacting us. Your contact will greatly help us fix these bugs.
