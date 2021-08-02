#include "stdio.h"
#include "stdlib.h"
#include "libpq-fe.h"
#include <iostream>
#include "string.h"
#include <ctime>
#include "windows.h"

#define N 91
#define ITERTIMES 100000

#define INT 0
#define NUMERIC 1 
#define CHAR 2
#define VARCHAR 3
#define DATE 4

extern void PQfinish(PGconn* conn);
extern void PQclear(PGresult* res);

int main()
{
	char str[33] = "D:\\Database\\exper\\exper1\\res.txt";
	char str1[34] = "D:\\Database\\exper\\exper1\\plan.txt";
	FILE* f_res = fopen(str, "w+");
	fclose(f_res);
	FILE* f_plan = fopen(str1, "w+");
	fclose(f_plan);
	for (int i = 22; i < 91; i++)
	{
		f_res = fopen(str, "at");
		f_plan = fopen(str1, "at");
		PGconn* conn = PQsetdbLogin("127.0.0.1", "5432", NULL, NULL, "imdbload", "", "");
		PGresult* result = NULL;
		char fpath[60];
		sprintf(fpath, "D:\\Database\\exper\\Agg\\%d.sql", i + 1);
		std::cout << i + 1 << "   ";
		fprintf(f_res, "%d\t", i + 1);
		fprintf(f_plan, "%d\n", i + 1);
		FILE* f_train = fopen(fpath, "r");
		char line[2048] = "";
		char line1[2048] = "";
		//read file success
		if (fgets(line, 2048, f_train) != NULL)
		{
			sprintf(line1, "explain %s", line);
		}
		//read file failed
		else
		{
			fclose(f_train);
			PQfinish(conn);
			break;
		}
		if (PQstatus(conn) == CONNECTION_BAD)
		{
			std::cout << PQerrorMessage(conn) << std::endl;
			fprintf(f_res, "%s\n", PQerrorMessage(conn));
			PQfinish(conn);
			fclose(f_train);
			fclose(f_res);
			fclose(f_plan);
			break;
		}
		else
		{
			std::string strMin;
			clock_t start, end;
			int loops = 0;
			int min_endtime = 0;
			std::string prev_res = "";
			std::string res = "";
			int endtime = 0;
			do
			{
				loops++;
				start = clock();
				result = PQexec(conn, line1);
				end = clock();
				endtime = (int)(end - start);
				int tuple_num = PQntuples(result);
				int field_num = PQnfields(result);
				if (tuple_num <= 0)
				{
					std::cout << "No result return." << std::endl;
					fprintf(f_res, "No result return.\n");
					PQfinish(conn);
					fclose(f_train);
					fclose(f_res);
					fclose(f_plan);
					break;
				}
				for (int i = 0; i < tuple_num; ++i)
				{
					for (int j = 0; j < field_num; ++j)
					{
						res.append(PQgetvalue(result, i, j));
						res.append("\n");
					}
				}
				if (min_endtime == 0)
				{
					min_endtime = endtime;
					strMin = res;
					fprintf(f_plan, "%s", strMin.c_str());
				}
				if (min_endtime > endtime)
				{
					min_endtime = endtime;
					strMin = res;
				}
				if (res == prev_res)
				{
					fprintf(f_res, "%d\n", min_endtime);
					fprintf(f_plan, "%s", strMin.c_str());
					break;
				}
				prev_res = res;
				res = "";
			} while (1);
			std::cout << "loops = " << loops << ", time = " << min_endtime << std::endl;
			PQfinish(conn);
		}
		fclose(f_res);
		fclose(f_plan);
	}
	return 0;
}