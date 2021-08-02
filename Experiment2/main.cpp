#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <iostream>
#include <string.h>
#include <ctime>
#include <windows.h>

extern void PQfinish(PGconn* conn);
extern void PQclear(PGresult* res);

#define N 91

void doAQuery(const char* str)
{
	clock_t start, end;
	PGconn* conn = PQsetdbLogin("127.0.0.1", "5432", NULL, NULL, "imdbload", "", "");
	PQexec(conn, "set client_encoding to \'utf8\';");
	PGresult* result = NULL;
	start = clock();
	result = PQexec(conn, str);
	end = clock();
	int row = PQntuples(result);
	if (row <= 0)
	{
		std::cout << "No result return." << std::endl;
		PQfinish(conn);
	}
	else
	{
		int endtime = (int)(end - start);
		std::cout << endtime << std::endl;
		PQfinish(conn);
	}
	return;
}

int main()
{
	bool* is_get = (bool*)malloc(N * sizeof(bool));
	int* arr = (int*)malloc(N * sizeof(int));
	memset(is_get, false, N * sizeof(bool));
	srand(time(0));
	int cnt = 0;
	int val;
	while (cnt < N)
	{
		do
		{
			val = rand() % N + 1;
		} while (is_get[val - 1]);
		is_get[val - 1] = true;
		arr[cnt++] = val;
	}
	free(is_get);
	FILE* f_log = fopen("D:\\Database\\exper\\exper2\\log.txt", "w+");
	fclose(f_log);
	f_log = fopen("D:\\Database\\exper\\exper2\\log.txt", "wt");
	for (int i = 0; i < 91; i++)
	{
		PGconn* conn = PQsetdbLogin("127.0.0.1", "5432", NULL, NULL, "imdbload", "", "");
		//PQexec(conn, "set client_encoding to \'utf8\';");
		PGresult* result = NULL;
		char fpath[60];
		sprintf(fpath, "D:\\Database\\exper\\NoAgg\\%d.sql", arr[i]);
		//PQexec(conn, "set enable_mergejoin = false;");
		std::cout << i << " " << fpath << "   ";
		fprintf(f_log, "%d\t", arr[i]);
		FILE* f_train = fopen(fpath, "r");
		char line[2048] = "";
		//read file success
		if(fgets(line, 2048, f_train) != NULL)
		{
			if (PQstatus(conn) == CONNECTION_BAD)
			{
				std::cout << PQerrorMessage(conn) << std::endl;
				fprintf(f_log, "%s\n", PQerrorMessage(conn));
				PQfinish(conn);
				fclose(f_train);
				break;
			}
			else
			{
				clock_t start, end;
				start = clock();
				result = PQexec(conn, line);
				end = clock();
				int row = PQntuples(result);
				if (row <= 0)
				{
					std::cout << "No result return." << std::endl;
					fprintf(f_log, "No result return.\n");
					PQfinish(conn);
					fclose(f_train);
					continue;
				}
				else
				{
					int endtime = (int)(end - start);
					std::cout << endtime  << std::endl;
					fprintf(f_log, "%d\n", endtime);
					PQfinish(conn);
					fclose(f_train);
				}
			}
		}
		//read file failed
		else
		{
			fclose(f_train);
			PQfinish(conn);
		}
	}
	fclose(f_log);
	free(arr);
	system("pause");
	return 0;
}