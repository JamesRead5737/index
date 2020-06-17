#include <mysql.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <signal.h>

int should_exit;

void
signal_handler(int signo)
{
        should_exit = 1;
}

int main(int argc, char **argv)
{
	int debug = 0;
	should_exit = 0;
        signal(SIGINT, signal_handler);
        signal(SIGKILL, signal_handler);

	MYSQL *con = mysql_init(NULL);
	MYSQL *con02 = mysql_init(NULL);

	if (con == NULL)
	{
		fprintf(stderr, "%s\n", mysql_error(con));
		exit(1);
	}

	if (con02 == NULL)
	{
		fprintf(stderr, "%s\n", mysql_error(con));
		exit(1);
	}

	if (mysql_real_connect(con, "localhost", "crawler", "1q2w3e4r", "indexes", 0, NULL, 0) == NULL)
	{
		fprintf(stderr, "%s\n", mysql_error(con));
		mysql_close(con);
		exit(1);
	}

	if (mysql_real_connect(con02, "localhost", "crawler", "1q2w3e4r", "indexes", 0, NULL, 0) == NULL)
	{
		fprintf(stderr, "%s\n", mysql_error(con));
		mysql_close(con);
		exit(1);
	}

	while (!should_exit)
	{

		char *sql = "SELECT id, title FROM crawl.crawled WHERE frontier = 0 AND ads = 0 AND indexed = 0 AND title LIKE '%%'";

		if (mysql_real_query(con, sql, (unsigned long)strlen(sql)))
		{
			fprintf(stderr, "mysql_real_query sql: %s %s\n", sql, mysql_error(con));
			if (mysql_errno(con) == CR_SERVER_GONE_ERROR)
			{
				mysql_close(con);
				con = mysql_init(NULL);
				if (con == NULL)
				{
					fprintf(stderr, "%s\n", mysql_error(con));
					exit(1);
				}

				if (mysql_real_connect(con, "localhost", "crawler", "1q2w3e4r", "indexes", 0, NULL, 0) == NULL)
			        {
                			fprintf(stderr, "%s\n", mysql_error(con));
                			mysql_close(con);
                			exit(1);
        			}

				continue;
			}
			else
			{
				mysql_close(con);
				exit(1);
			}
		}

		MYSQL_RES *result = mysql_use_result(con);
	
		if(result == NULL)
		{
			fprintf(stderr, "mysql_use_result %s\n", mysql_error(con));
			mysql_close(con);
			exit(1);
		}

		MYSQL_ROW row;

		while ((row = mysql_fetch_row(result)))
		{
			if (should_exit)
				break;
			int id = atoi(row[0]);
			char *token;
			//printf("%s\n", row[1]);
			fflush(stdout);
			token = strtok(row[1], " \r\n\t");
			if (token != NULL)
			{
				char escaped_token[(strlen(token)*2)+1];
				if (!mysql_real_escape_string(con02, escaped_token, token, strlen(token)))
               	 		{
               	 		}

				int size = strlen(escaped_token) + strlen("CREATE TABLE IF NOT EXISTS `` (`id` int NOT NULL, PRIMARY KEY (`id`))") + 1;
				char *sql02 = (char *) malloc (size);
				int ret = snprintf(sql02, size, "CREATE TABLE IF NOT EXISTS `%s` (`id` int NOT NULL, PRIMARY KEY (`id`))", escaped_token);
				if (ret >= 0 && ret <= size)
                		{
	
        	        	}
                		else
                		{
                		        if (debug)
                		                fprintf(stderr, "%s was too large for buffer\n", escaped_token);
                		}

				if (mysql_real_query(con02, sql02, (unsigned long)strlen(sql02)))
				{
					fprintf(stderr, "mysql_real_query sql: %s %s\n", sql02, mysql_error(con02));
				}

				free(sql02);

				int nDigits = floor(log10(abs(id))) + 1;
				size = strlen(escaped_token) + strlen("INSERT IGNORE INTO `` (id) VALUES ()") + nDigits + 1;
				sql02 = (char *) malloc (size);
				ret = snprintf(sql02, size, "INSERT IGNORE INTO `%s` (id) VALUES (%d)", escaped_token, id);
				if (ret >= 0 && ret <= size)
				{}
                		else
                		{
                		        if (debug)
                	       		         fprintf(stderr, "%s was too large for buffer\n", escaped_token);
                		}
				if (mysql_real_query(con02, sql02, (unsigned long)strlen(sql02)))
				{
					fprintf(stderr, "mysql_real_query sql: %s %s\n", sql02, mysql_error(con02));
				}
				free(sql02);
			}

			while (token != NULL)
			{
				token = strtok(NULL, " \r\n\t");
				if (token != NULL)
				{
					char escaped_token[(strlen(token)*2)+1];
					if (!mysql_real_escape_string(con02, escaped_token, token, strlen(token)))
                			{
                			}
	
					int size = strlen(escaped_token) + strlen("CREATE TABLE IF NOT EXISTS `` (`id` int NOT NULL, PRIMARY KEY (`id`))") + 1;
					char *sql02 = (char *) malloc (size);
					int ret = snprintf(sql02, size, "CREATE TABLE IF NOT EXISTS `%s` (`id` int NOT NULL, PRIMARY KEY (`id`))", escaped_token);
					if (ret >= 0 && ret <= size)
       	 	        		{}
        	        		else
        	        		{
						if (debug)
							fprintf(stderr, "%s was too large for buffer\n", escaped_token);
                			}

					if (mysql_real_query(con02, sql02, (unsigned long)strlen(sql02)))
					{
						fprintf(stderr, "%s\n", mysql_error(con02));
					}

					free(sql02);

					int nDigits = floor(log10(abs(id))) + 1;
					size = strlen(escaped_token) + strlen("INSERT IGNORE INTO `` (id) VALUES ()") + nDigits + 1;
					sql02 = (char *) malloc (size);
					ret = snprintf(sql02, size, "INSERT IGNORE INTO `%s` (id) VALUES (%d)", escaped_token, id);
					if (ret >= 0 && ret <= size)
                			{}
                			else
                			{
                			        if (debug)
                        			        fprintf(stderr, "%s was too large for buffer\n", escaped_token);
                			}
					if (mysql_real_query(con02, sql02, (unsigned long)strlen(sql02)))
					{
						fprintf(stderr, "mysql_real_query sql: %s %s\n", sql02, mysql_error(con02));
					}
					free(sql02);
				}
			}
			
			int nDigits = floor(log10(abs(id))) + 1;
			int size = strlen("UPDATE crawl.crawled SET indexed = 1 WHERE id = ") + nDigits + 1;
			char *sql02 = (char *) malloc (size);
			int ret = snprintf(sql02, size, "UPDATE crawl.crawled SET indexed = 1 WHERE id = %d", id);
			if (ret >= 0 && ret <= size)
			{}
			else
			{
				if (debug)
					fprintf(stderr, "%d was too large for buffer\n", id);
			}
			if (mysql_real_query(con02, sql02, (unsigned long)strlen(sql02)))
			{
				fprintf(stderr, "mysql_real_query sql: %s %s\n", sql02, mysql_error(con02));
			}
			free(sql02);
		}
		mysql_free_result(result);
	}

	mysql_close(con);
}
