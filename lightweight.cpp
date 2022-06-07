/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "lightweight.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ctime>

#if defined(_WIN32) || defined(_WIN64)
  #define strcasecmp _stricmp
#endif

int main(int argc, char** argv)
{
	int rc = 0;
	FILE* log_fp;
	if ((log_fp = fopen("db.log", "a")) == NULL)
	{
		printf("ERROR: unable to create/open log file\n");
	}
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;


	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}

	rc = initialize_tpd_list();

  if (rc)
  {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  }
	else
	{
    rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}
		else
		{
			if (log_fp != NULL)
			{
				if (logging)
				{
					fprintf(log_fp, "%s ", get_timestamp());
					fprintf(log_fp, "\"%s\n", argv[1]);
				}
				else if (backup)
                {
                    fprintf(log_fp, "%s ", get_timestamp());
                    token_list* cur = tok_list;
                    while((cur = cur->next)->tok_value != K_BACKUP);
                    fprintf(log_fp, "BACKUP %s\n", cur->next->tok_string);
                }
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			tmp_tok_ptr = tok_ptr->next;
			free(tok_ptr);
			tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}
char* get_timestamp()
{
    char *buff = (char*) malloc(20);
    time_t now = time(NULL);
    strftime(buff, 20, "%Y%m%d%H%M%S", localtime(&now));
    return buff;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				printf("INVALID STATEMENT, 121: %s\n", temp_string);
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						printf("INVALID STATEMENT, 156: %s\n", temp_string);
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				printf("INVALID STATEMENT, 184: %s\n", temp_string);
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
				printf("INVALID STATEMENT, 243: %s\n", temp_string);
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				printf("INVALID STATEMENT, 271: %s\n", temp_string);
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
					((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_SELECT) &&
					((cur->next != NULL) && (cur->next->tok_value == S_STAR) || (cur->next != NULL) && (cur->next->tok_value == IDENT)))
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
    else if ((cur->tok_value == K_DELETE) && ((cur->next != NULL) &&(cur->next->tok_value == K_FROM)))
    {
        printf("DELETE statement\n");
        cur_cmd = DELETE;
        cur = cur->next;
    }
    else if ((cur->tok_value == K_UPDATE) && ((cur->next != NULL) &&(cur->next->tok_value == IDENT)))
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_BACKUP) && ((cur->next != NULL) && (cur->next->tok_value == K_TO)))
	{
		printf("BACKUP statement\n");
		cur_cmd = BACKUP;
		backup = true;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_RESTORE) && (cur->next != NULL) && (cur->next->tok_value = K_FROM))
	{
		printf("RESTORE statement\n");
		cur_cmd = RESTORE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_ROLLFORWARD))
	{
		printf("ROLLFORWARD statement\n");
		cur_cmd = ROLLFORWARD;
		cur = cur->next;
	}
	else
  	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd == SELECT || cur_cmd == BACKUP || cur_cmd == RESTORE || cur_cmd == ROLLFORWARD)
		logging = false;

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
			case INSERT:
						rc = sem_insert(cur);
						break;
			case SELECT:
						rc = sem_select(cur);
						break;
		    case DELETE:
		                rc = sem_delete(cur);
		                break;
			case UPDATE:
						rc = sem_update(cur);
						break;
			case BACKUP:
						rc = sem_backup(cur);
						break;
			case RESTORE:
						rc = sem_restore(cur);
						break;
			case ROLLFORWARD:
						rc = sem_rollforward(cur);
						break;
			default:
					; /* no action */
		}
	}
	return rc;
}


int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list; 
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{		//check if table exists
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{		
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)//no left paren after table name spec
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  {
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar() 
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) && (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
															 sizeof(cd_entry) *	tab_entry.num_columns;
				  	tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);
						table_file_header* header = (table_file_header*) malloc(sizeof(table_file_header));
						memset(header, '\0', sizeof(table_file_header));
						build_table_file_header_struct(new_entry, col_entry, header);
						write_table_to_file(new_entry, col_entry, header, NULL);
						free(new_entry);
						free(header);
					}
				}
			}
		}
	}
  return rc;
}

int sem_insert(token_list *t_list) 
{
	int rc = 0;
	token_list *cur;
	cur = t_list;
	int cur_id = 0;
	tpd_entry* tab_entry;
	
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
	{
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		printf("INVALID STATEMENT, 656: %s\n", cur->tok_string);
	}
	else {
		cur = cur->next;
		if (cur->tok_value != K_VALUES)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			printf("INVALID STATEMENT: %s\n", cur->tok_string);
		}
		else 
		{
			cur = cur->next;

			table_file_header* header;
			cd_entry* columns;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				printf("INVALID STATEMENT: %s\n", cur->tok_string);
			}
			else 
			{
				cur = cur->next;

				FILE* fp;
				char *filename = (char*) malloc(sizeof(tab_entry->table_name) + 4);
				
				strcpy(filename, tab_entry->table_name);
				strcat(filename, ".tab");
				if ((fp = fopen(filename, "ab+")) == NULL) 
				{
					printf("ERROR: unable to read table from file\n");
					return FILE_OPEN_ERROR;
				}
				header = (table_file_header*) malloc(sizeof(table_file_header));

				fread((void*) header, sizeof(table_file_header), 1, fp);
				fseek(fp, tab_entry->cd_offset, SEEK_CUR);
				columns = load_columns_from_file(tab_entry);



				char buffer[header->record_size];
				memset(buffer, '\0', header->record_size);
				char** records;
				records = load_table_records(tab_entry, header);
				int elements_written = 0;
				int index = 0;
				while(!rc)
				{
					if (elements_written > tab_entry->num_columns)
					{
						rc = COLUMN_NOT_EXIST;
					} 
					else if (cur->tok_value == S_COMMA) 
					{
						cur = cur->next;
						continue;
					}
					else if (cur->tok_value == STRING_LITERAL)
					{
						if (strlen(cur->tok_string) == 0 || strlen(cur->tok_string) > columns[elements_written].col_len)
						{
							rc = INVALID_COLUMN_LENGTH;
							cur->tok_value = INVALID_STATEMENT;
							printf("ERROR: invalid length for column: %s\n", columns[elements_written].col_name);
							return rc;
						}
						else if (columns[elements_written].col_type == T_CHAR || columns[elements_written].col_type == T_VARCHAR)
						{
							buffer[index++] = (unsigned char) strlen(cur->tok_string);
							int i = 0;
							for (i; i < strlen(cur->tok_string); i++)
								buffer[index++] = cur->tok_string[i];

							index += (columns[elements_written++].col_len - i);
							cur = cur->next;

						}
						else
						{
							
							printf("ERROR: invalid type in value: %s\n", cur->tok_string  );
							rc = INVALID_TYPE_NAME;
							return rc;
						}
					}
					else if (cur->tok_value == INT_LITERAL)
					{

						if (columns[elements_written].col_type != T_INT)
						{
							printf("ERROR: invalid type in value: %s\n", cur->tok_string);
							rc = INVALID_TYPE_NAME;
							return rc;
						}
						else
						{
							int value = atoi(cur->tok_string);
							unsigned char *bytes = (unsigned char*) calloc(1, sizeof(int));
							memcpy((void*) bytes, (void*) &value, 8);


							int i;
							for (i = 0; i < 4; i++)
								memcpy(&buffer[index++], &bytes[i], 1);

							elements_written++;
							cur = cur->next;
						}
					}
					else if (cur->tok_value == K_NULL)
					{
						if (columns[elements_written].not_null)
						{
							rc = NULL_INSERT;
							cur->tok_value = INVALID_STATEMENT;
						}
						else
						{
							
							index += (columns[elements_written++].col_len);//length value is already 0, so we can just skip to the next column
							elements_written++;
						}
					}
					else if (cur->tok_value == S_RIGHT_PAREN)
					{
						 rc = 0;
						 break;// we're done

					}
					else
					{
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
					}
					
				}

				printf("copying buffer to record in memory.\n");
				memset(records[header->num_records], '\0', header->record_size);
				memcpy((void*) records[header->num_records++], (void*) buffer, header->record_size);

				header->file_size += header->record_size;

				//for (int i = 0; i < header->record_size; i++)
				//		records[header->num_records++][i] = buffer[i];
				printf("memcpy successful, writing records to file..\n");
				if (!write_table_to_file(tab_entry, columns, header, records))
				{
					printf("ERROR: unable to write records to table file\n");
					rc = FILE_OPEN_ERROR;
				}
				
				free(header);
				free(filename);
				free(columns);
				fflush(fp);
				fclose(fp);
			
			}
		}
	}
	return rc;
}

//returns a file name for a given table
void get_table_file_name(char* table_name, char* filename)
{
	strcpy(filename, table_name);
	strcat(filename, ".tab");
}

cd_entry* load_columns_from_file(tpd_entry* tab_entry)
{
	FILE* fp;
	if ((fp = fopen("dbfile.bin", "rb")) == NULL)
	{
		printf("ERROR: unable to open database file\n");
		return NULL;
	}

	cd_entry* columns = (cd_entry*) calloc(tab_entry->num_columns, sizeof(cd_entry));

	fseek(fp, sizeof(tpd_list), SEEK_SET);

	for (int i = 0; i < tab_entry->num_columns; i++)
		fread((void*) &columns[i], sizeof(cd_entry), 1, fp);
		
	fclose(fp);
	return columns;
}

table_file_header* load_file_header(char* tablename)
{
    FILE* fp;
    char *filename = (char*) malloc(sizeof(tablename) + 4);
    table_file_header* header;

    strcpy(filename, tablename);
    strcat(filename, ".tab");
    if ((fp = fopen(filename, "ab+")) == NULL)
    {
        printf("ERROR: unable to read table from file\n");
        return NULL;
    }
    header = (table_file_header*) malloc(sizeof(table_file_header));

    fread(header, sizeof(table_file_header), 1, fp);

    return header;
}

//loads the records of a table + 100 records worth of space.
//returns a pointer to the beginning of the block of memory

char** load_table_records(tpd_entry* tab_entry, table_file_header* tf_header)
{	
	
	FILE* fp;
	char* filename = (char*) malloc(sizeof(tab_entry->table_name) + 5);

	get_table_file_name(tab_entry->table_name, filename);

	if ((fp = fopen(filename, "rbc")) == NULL)
	{
		printf("ERROR: UNABLE TO OPEN FILE FOR TABLE %s\n", tab_entry->table_name);
		return NULL;
	}

	char **records;
	records = (char**) calloc(sizeof(char**), 100);
	//memset(records, '\0', sizeof(char) * tf_header->record_size * 100);

	for (int i = 0; i < 100; i++)
	
		records[i] = (char*) calloc((size_t) tf_header->record_size, 1);
	
	if (tf_header->num_records == 0) return records;
    unsigned char buffer[tf_header->record_size];
    memset(buffer, '\0', tf_header->record_size);
	fseek(fp, tf_header->record_offset, SEEK_SET);
	int i = 0;
	for (i; i < tf_header->num_records; i++) 
	{
		fread((void*) buffer, (size_t) tf_header->record_size, 1, fp);
		memcpy ((void*) records[i], (void*) buffer, tf_header->record_size);
	}
	fclose(fp);
	return records;
}

void build_table_file_header_struct(tpd_entry* table, cd_entry* columns, table_file_header* header) 
{

	for (int i = 0; i < table->num_columns; i++) {

		switch(columns->col_type) {
			case T_INT:
				header->record_size += 5;

			break;
			case T_CHAR:
			case T_VARCHAR:
				header->record_size += 1 + columns->col_len;
			break;
		}

		columns++;
	}

	while (header->record_size % 4 != 0)
		header->record_size++;
	header->record_offset = sizeof(table_file_header);
	header->file_size = sizeof(table_file_header);

}

//writes a table to a .tab file
//returns 0 if successfully written to file, -1 if there was any write errors.
int write_table_to_file(tpd_entry* tab_entry, cd_entry* columns, table_file_header* tf_header, char** records)
{
	FILE *fp = NULL;
	char *filename = (char*) malloc(sizeof(tab_entry->table_name) + 4);
	strcpy(filename, tab_entry->table_name);
	strcat(filename, ".tab");
	//int bytes = 0;

	/*calculate file sizes*/

	if ((fp = fopen(filename, "wb")) == NULL) 
	{
		printf("ERROR: unable to open file\n ");
		return 0;
	}
	fwrite((void*) tf_header, sizeof(table_file_header), 1, fp);
	if (ferror(fp))//check for write error
	{
		remove(tab_entry->table_name);
		printf("ERROR: unable to write to .tab file\n");
		return 0;
	}

	for (int i = 0; i < tf_header->num_records; i++)
	{
		char buffer[tf_header->record_size];
		memset(buffer, '\0', (size_t) tf_header->record_size);
		memcpy(buffer, records[i], (size_t) tf_header->record_size);
		fwrite(buffer, (size_t) tf_header->record_size, 1, fp);
	}

	fflush(fp);
	fclose(fp);

	return 1;
}

int sem_select(token_list *t_list)
{

	token_list *cur = t_list;
	int rc = 0;
	bool projection;
	tpd_entry *tab_entry = NULL;
	tab_entry = (tpd_entry*) malloc(sizeof(tpd_entry));
	cd_entry* p_columns = NULL;
	token_list* column_names = NULL;
	int p_record_size = 0;
	int num_pcols = 0;
	if (cur->tok_value != S_STAR)
	{
		int count = 0;
		projection = true;
		column_names = cur;
	} else projection = false;

	cur = cur->next;
	if (cur->tok_value != K_FROM)
	{
		rc = INVALID_COLUMN_NAME;
		cur->tok_value = INVALID;
		printf("INVALID STATEMENT: %s\n", cur->tok_string);
	}
	else
	{
		cur = cur->next;
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = TABLE_NOT_EXIST;
			printf("TABLE DOES NOT EXIST: %s\n", cur->tok_string);
		}
		else
		{
			table_file_header* header;
			header = load_file_header(tab_entry->table_name);

			cd_entry* columns;
			columns = load_columns_from_file(tab_entry);
			if (projection) p_columns = get_projected_columns(columns, column_names, tab_entry->num_columns, &p_record_size, &num_pcols);
			char** records = load_table_records(tab_entry, header);
			cur = cur->next;
			if (!projection)
			{
                if (cur->tok_value == K_WHERE)
                {
                    int records_found = 0;
					char** selection = select_records(columns, records, cur, header->num_records, tab_entry->num_columns, header->record_size, &records_found);

					print_records(selection, columns, records_found, header->record_size, tab_entry->num_columns);
                }
				else if (cur->tok_class == terminator)
				{
					print_records(records, columns, header->record_size, header->num_records,  tab_entry->num_columns);
				}

			}
			else
			{
				if (cur->tok_value != K_WHERE)
				{
                    //int records_found = 0;
                    //char** selection = select_records(columns, records, cur, header->num_records, tab_entry->num_columns, header->record_size, &records_found);

                    char** projected = project_records(columns, p_columns, tab_entry->num_columns, num_pcols, records, header->num_records, p_record_size);
                    print_records(projected, p_columns, p_record_size, header->num_records, num_pcols);
				}
				else
				{
                    int records_found = 0;
                    char** selection = select_records(columns, records, cur, header->num_records, tab_entry->num_columns, header->record_size, &records_found);


                    char** projected = project_records(columns, p_columns, tab_entry->num_columns, num_pcols, selection, records_found, p_record_size);
                    print_records(projected, p_columns, p_record_size, records_found, num_pcols);
				}
			}
		}
	}
	if (column_names != NULL) free (column_names);
	free (cur);
	if (p_columns != NULL) free (p_columns);

    return rc;
}

int sem_delete(token_list *tok)
{
    table_file_header *header;
    token_list *cur = tok;
    tpd_entry *tab_entry;
    cd_entry *columns;
    int rc = 0;
    char** records;


	if (cur->tok_value != K_FROM)
	{
		printf("ERROR: invalid statement: %s", cur->tok_string);
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;
		header = load_file_header(cur->tok_string);
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			printf("ERROR: table %s does not exist\n", cur->tok_string);
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;

		}
		else
		{
			cur = cur->next;
			columns = load_columns_from_file(tab_entry);
			records = load_table_records(tab_entry, header);
			if (cur->tok_value == EOC)
			{

				delete_records(NULL, columns, tab_entry->num_columns, header, records);
				write_table_to_file(tab_entry, columns, header, records);
			}
			else if (cur->tok_value == K_WHERE)
			{
				delete_records(cur, columns, tab_entry->num_columns, header, records);
				write_table_to_file(tab_entry, columns, header, records);
			} else
			{
				printf("ERROR: invalid statement\n");
			}
		}
	}

    return rc;
}

int sem_update(token_list *tok)
{
    token_list *cur = tok;
    int rc = 0;
    tpd_entry *tab_entry = NULL;
    tab_entry = (tpd_entry*) malloc(sizeof(tpd_entry));
    table_file_header *tf_header;
    cd_entry* columns = NULL;
    char** records;
    cd_entry* update_columns;
    int num_updated = 0;


    if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
    {
        printf("ERROR: table %s does not exist\n", cur->tok_string);
        rc = TABLE_NOT_EXIST;
        cur->tok_value = INVALID;
    }
    else
    {
        tf_header = load_file_header(tab_entry->table_name);
        columns = load_columns_from_file(tab_entry);
        records = load_table_records(tab_entry, tf_header);
        cur = cur->next;
        if (cur->tok_value != K_SET)
        {
            printf("ERROR: Invalid statement, expected 'SET', got %s\n", cur->tok_string);
            rc = INVALID_STATEMENT;
            cur->tok_value = INVALID;
        }
        else
        {
            cur = cur->next;
            if (cur->tok_value != IDENT)
            {
                printf("ERROR: Invalid statement, expected identifier\n");
                rc = INVALID_STATEMENT;
                cur->tok_value = INVALID;
            }
            else
            {

                t_list* counter = cur;
                bool where = false;
                int num_columns = 0;
                while ((counter->tok_value != EOC && counter->tok_value != K_WHERE) && !rc && cur != NULL)
				{
					num_columns++;
					counter = counter->next;
					if (counter->tok_value != S_EQUAL)
					{
						rc = INVALID_STATEMENT;
						printf("ERROR: expected '=', got %s", counter->next->tok_string);
						counter->next->tok_value = INVALID;
					}
					counter = counter->next;
					if (counter->tok_value != STRING_LITERAL && counter->tok_value != INT_LITERAL && counter->tok_value != K_NULL)
					{
						rc = INVALID_STATEMENT;
						printf("ERROR: expected string literal, int literal, or null keyword, got %s\n", counter->tok_string);
					}
					counter = counter->next;
				}
                if (counter->tok_value == K_WHERE) where = true;

                update_columns = (cd_entry*) calloc((size_t ) num_columns, sizeof(cd_entry));

                free(counter);
                t_list* col_cur = cur;
                int col_index = 0;
                while (col_cur->tok_value!= EOC)
                {
                	if (col_cur->tok_value == S_EQUAL
						|| col_cur->tok_value == INT_LITERAL
						|| col_cur->tok_value == STRING_LITERAL
						|| col_cur->tok_value == K_NULL)
                	{
                		col_cur = col_cur->next;
                		continue;
                	}
                    //find the column specified
                    for (int i = 0; i < tab_entry->num_columns; i++)
                    {
                        if (strcasecmp(columns[i].col_name, cur->tok_string) == 0)
                        {
                            memcpy((void*) &update_columns[col_index++], (void*) &columns[i], sizeof(cd_entry));//copy columns for update
                            break;

                        }

                    }

                    col_cur = col_cur->next;//skip to next statement


                }

                t_list *val_cur = cur->next->next;//go to first value
                free(col_cur);
                if (where)
                {
                    t_list *where_cur = cur;
                    while ((where_cur = where_cur->next)->tok_value != K_WHERE );
                    rc = update_records(cur, val_cur, where_cur, columns, tab_entry->num_columns, update_columns, num_columns, records, tf_header->num_records, &num_updated);
                }
                else
				{
					rc = update_records(cur, val_cur, NULL, columns, tab_entry->num_columns, update_columns, num_columns, records, tf_header->num_records, &num_updated);
				}

				if (!write_table_to_file(tab_entry, columns, tf_header, records))
				{
					printf("ERROR: unable to write records to table file\n");
					rc = FILE_OPEN_ERROR;
				}



            }
        }

    }
    return rc;

}

int sem_backup(token_list *tok)
{
	token_list *cur = tok;
	int rc = 0;

    if (cur->tok_value != K_TO)
	{
		printf("ERROR: expected 'TO', got '%s'\n", cur->tok_string);
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;

	}
	else
	{
		cur = cur->next;
		if (cur->tok_value != IDENT)
		{
			printf("ERROR: expected identifier at '%s'\n", cur->tok_string);
			rc = INVALID_STATEMENT;
			cur->tok_value =  INVALID;

		}
		else
		{
			rc = create_image(cur);
		}
	}
	free(cur);
	return rc;

}

int file_exist (char *filename)
{
	struct stat   buffer;
	return (stat (filename, &buffer) == 0);
}

int create_image(token_list *tok)
{
	token_list *cur = tok;
	char img_filename[MAX_IDENT_LEN];
	FILE* img_fp = NULL;
	FILE* db_fp = NULL;
	int rc = 0;
	bool succesful = false;//true if we successfully wrote the image file

	if (cur->tok_value != IDENT)
	{
		printf("ERROR: expected identifier at '%s'\n", cur->tok_string);
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;

	}
	else
	{
		strcpy(img_filename, cur->tok_string);
		if (file_exist(img_filename))
		{
			printf("ERROR: Image file '%s' already exists.\n", img_filename );
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			if ((img_fp = fopen(img_filename, "wb")) == NULL)
			{
				printf("ERROR: unable to create file '%s'\n ", img_filename);
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				if ((db_fp = fopen("dbfile.bin", "rb")) == NULL)
				{
					printf("ERROR: unable to open 'dbfile.bin'\n");
					rc = FILE_OPEN_ERROR;
				}
				else
				{
				    fseek(db_fp, 0, SEEK_END);
					long db_length = ftell(db_fp);
                    fseek(db_fp, 0, SEEK_SET);
					char* dbfile_buffer = (char*) calloc(1, (size_t) db_length);
					fread(dbfile_buffer, 1, (size_t) db_length, db_fp);
					fwrite(dbfile_buffer, 1, (size_t) db_length, img_fp);
					tpd_entry* cur_tab = &(g_tpd_list->tpd_start);
					char table_file_name[FILENAME_MAX];
					for (int i = 0; i < g_tpd_list->num_tables; i++)
					{
						get_table_file_name(cur_tab->table_name, table_file_name);
						FILE* tab_fp;
						table_file_header* tf_header = load_file_header(cur_tab->table_name);

						if ((tab_fp = fopen(table_file_name, "rb")) == NULL)
						{
							printf("ERROR: unable to open table file '%s'\n", table_file_name);
							rc = FILE_OPEN_ERROR;
							break;
						}

						char table_buffer[tf_header->file_size];

						fread(table_buffer, 1, (size_t) tf_header->file_size, tab_fp);
						fprintf(img_fp, "%d", tf_header->file_size);
						fwrite(table_buffer, 1, (size_t) tf_header->file_size, img_fp);
                        cur_tab = (tpd_entry*)((char*)cur_tab + cur_tab->tpd_size);

					}
					succesful = true;

				}
			}
		}

	}
	if (!succesful)
	    remove(img_filename);
	if (img_fp != NULL)  fflush(img_fp);
	free(cur);
	if (db_fp == NULL) free(db_fp);
	return rc;

}

int sem_rollforward(token_list *tok)
{

}

int sem_restore(token_list *tok)
{
    token_list* cur = tok;
    int rc = 0;
    char filename[MAX_IDENT_LEN];
    if (cur->tok_value != K_FROM)
    {
        printf("ERROR: expected 'FROM', got '%s'\n", cur->tok_string );
        rc = INVALID_STATEMENT;
        cur->tok_value = INVALID;
    }
    else
    {
        cur = cur->next;
        if (cur->tok_value != IDENT)
        {
            printf("ERROR: expected identifier, got '%s'\n", cur->tok_string);
            rc = INVALID_STATEMENT;
            cur->tok_value = INVALID;
        }
        else
        {

            strcpy(filename, cur->tok_string);
            cur = cur->next;

            if (cur->tok_value == EOC)
            {
                restore_from_image(filename, false);
            }
            else if (cur->tok_value == K_WITHOUT_RF)
            {
                restore_from_image(filename, true);
            }
            else
            {
                printf("ERROR: expected 'WITHOUT RF', got '%s'\n", cur->tok_string);
                rc = INVALID_STATEMENT;
                cur->tok_value = INVALID;
            }
        }
    }
    return rc;
}

int restore_from_image(char* img_filename, bool without_rf)
{

    FILE* log_fp;
    FILE* img_fp;
    FILE* db_fp;
    FILE* tab_fp;
    int rc = 0;
    if ((log_fp = fopen("db.log", "r+")) == NULL)
    {
        printf("ERROR: unable to open db.log file\n");
        rc = FILE_OPEN_ERROR;

    }
    else
    {
        if (!file_exist(img_filename))
        {
            printf("ERROR: image file '%s' does not exist\n", img_filename);
            rc = FILE_OPEN_ERROR;

        }
        else if ((img_fp = fopen(img_filename, "r")) == NULL)
        {
            printf("ERROR: Unable to open image file '%s'\n", img_filename);
            rc = FILE_OPEN_ERROR;
        }
        else
        {
            if ((db_fp = fopen("dbfile.bin", "w")) == NULL)
            {
                printf("ERROR: failed to open 'dbfile.bin'\n");
                rc = FILE_OPEN_ERROR;

            }
            else
            {
                int db_file_size;
                fread(&db_file_size, sizeof(int), 1, img_fp);
                char* dbfile_buffer[db_file_size];
                fread(dbfile_buffer, db_file_size, 1, img_fp);
                fseek(img_fp, 0, SEEK_SET);
                fread(g_tpd_list, db_file_size, 1, img_fp);
                fwrite(dbfile_buffer, db_file_size, 1, db_fp);
                tpd_entry* tab_entry = &g_tpd_list->tpd_start;
                while (!feof(img_fp))
                {
                    int tab_size;
                    fread(&tab_size, sizeof(int), 1, img_fp);
                    table_file_header* tf_header = (table_file_header*) calloc(tab_size, 1);
                    fread(tf_header, tab_size, 1, img_fp);
                    char tab_filename[MAX_IDENT_LEN];
                    get_table_file_name(tab_entry->table_name, tab_filename);
                    if ((tab_fp = fopen(tab_filename, "w")) == NULL)
                    {
                        printf("ERROR: unable to restore from image '%s'; Unable to open table file '%s.tab'\n", img_filename, tab_filename);
                        rc = FILE_OPEN_ERROR;
                    }
                    else
                    {
                        fwrite(tf_header, sizeof(tf_header), 1, tab_fp);
                        tab_entry = (tpd_entry*)((char*)tab_entry + tab_entry->tpd_size);
                    }

                }


            }
        }
    }

    return rc;
}

int update_records(token_list* tok, token_list *val_cur, token_list *where, cd_entry* columns, int num_columns, cd_entry *update_columns, int num_u_columns, char** records, int num_records, int *num_updated )
{

	int rc = 0;
	t_list *cur = val_cur;//save the cursor at the first value
	if (where == NULL)
	{
		int record_index = 0;
		int record_mark = 0;
		int upcol_index = 0;

		while(cur != NULL && cur->tok_value != EOC)
		{
			record_mark = 0;
			for (int j = 0; j < update_columns[upcol_index].col_id; j++)
			{
				record_mark += columns[j].col_len;
				if (columns[j].col_type == T_CHAR || columns[j].col_type == T_VARCHAR) record_mark++;
			}

			if (cur->tok_value == K_NULL)
			{
				if (!update_columns[upcol_index++].not_null)
				{
					printf("ERROR: null update on a NOT NULL column\n");
					cur->tok_value = INVALID;
					rc = INVALID_STATEMENT;
				}
				else
				{

					record_index = record_mark;
					for (int i = 0; i < num_records; i++)
					{

						memset(&records[i][record_index], '\0', (size_t) update_columns[upcol_index++].col_len);
					}
				}
			}
			else if (cur->tok_value == STRING_LITERAL)
			{

				record_index = record_mark;
				for (int i = 0; i < num_records; i++)
				{

					if (update_columns[upcol_index].col_type != T_VARCHAR &&
						update_columns[upcol_index].col_type != T_CHAR)
					{
						printf("ERROR: null update on a NOT NULL column\n");
						cur->tok_value = INVALID;
						rc = INVALID_STATEMENT;
					}
					int size = (int) strlen(cur->tok_string);
					records[i][record_index++] = (unsigned char) size;
					memcpy(&records[i][record_index], cur->tok_string, (size_t) size);
				}

			}
			else if (cur->tok_value == INT_LITERAL)
			{

				record_index = record_mark;
				for (int i = 0; i < num_records; i++)
				{


					if (update_columns[upcol_index].col_type != T_INT)
					{
						printf("ERROR: invalid type in value: %s\n", cur->tok_string);
						rc = INVALID_TYPE_NAME;
						cur->tok_value = INVALID;
					} else
					{
						int value = atoi(cur->tok_string);
						unsigned char *bytes = (unsigned char *) calloc(1, sizeof(int));
						memcpy((void *) bytes, (void *) &value, sizeof(int));

						memcpy(&records[i][record_index], bytes, sizeof(int));
					}
				}
			}

			cur = cur->next;
		}

		*num_updated = num_records;
	}
	else
	{

	}

	return rc;
}
//selects records based on a conditional
//tok should be everything after the WHERE statement
char** select_records(cd_entry* cols, char** records, token_list* tok, int num_records, int num_cols, int record_size, int *records_found)
{
    token_list* cur = tok;

    int sel_index = 0;
	char** selection = (char**) calloc(sizeof(char*),  num_records);
	for (int i = 0; i < num_records; i++ )
		selection[i] = (char*) malloc((size_t) record_size);//allocate buffer for our selection of record
	for (int i = 0; i < num_records; i++)
	{
        cur = tok;

        token_value cond = K_NULL;
		while (cur->tok_class != terminator)
		{

			if ((cur->next->tok_value == K_AND || cur->next->tok_value == K_OR) && cond == K_NULL)
				cond = (token_value) cur->next->tok_value;
			if (cur->tok_value == IDENT)
			{
				for (int j = 0; j < num_cols; j++)
				{
					if (strncmp(toupper(cols[j].col_name), toupper(cur->tok_string), sizeof(cur->tok_string)) == 0)
					{
						if (cols[j].col_type == T_CHAR || cols[j].col_type ==  T_VARCHAR)
						{
							int rec_index = 0;

							for (int k = 0; k < j; k++)
							{
								rec_index += cols[k].col_len;
								if (cols[k].col_type == T_CHAR || cols[j].col_type == T_VARCHAR) rec_index++;
							}
							int size = 0;
							size = (int) records[i][rec_index++];
							char buffer[size];
							memcpy(buffer, &records[i][rec_index], (size_t) size);
							cur = cur->next;
							if (cur->tok_value != S_EQUAL)
							{
								return NULL;
							}
							cur = cur->next; // move to condition value
							if (strncmp(toupper(buffer), toupper(cur->tok_string), (size_t) size) == 0)
							{

								if (cond == K_OR || cond == K_NULL )//its true, condition has been satisfied
                                {
                                    memcpy(selection[sel_index++], records[i], (size_t) record_size);
									(*records_found)++;
                                    break;
                                }
                                else
                                {
                                    continue;//and
                                }
							}
							else if (cond == K_AND)//false with an AND
							{

								break;

							}
						}
						else if (cols[j].col_type == T_INT)//we have an integer column
						{
							char buffer[4];
							int rec_index = 0;
							int k = 0;
							for (k; k < j; k++)
							{
								rec_index += cols[k].col_len;
								if (cols[j].col_type == T_CHAR || cols[j].col_type == T_VARCHAR) rec_index++;
							}
							//rec_index+= k;
							memcpy(buffer, &records[i][rec_index], sizeof(int));
							int buf_value = (int) *buffer;


							cur = cur->next;
                            int cond_value = atoi(cur->next->tok_string);
							switch(cur->tok_value)
							{

								case S_EQUAL:

									if (cond_value == buf_value)
									{
                                        if (cond == K_AND)
                                        {
                                            continue;
                                        }
                                        else if (cond == K_OR || cond == K_NULL) {
											(*records_found)++;
                                            memcpy(selection[sel_index++], records[i], (size_t) record_size);
                                            break;
                                        }
									}
									break;
								case S_LESS:
									if (cond_value > buf_value)
									{
                                        if (cond == K_AND)
                                        {
                                            continue;
                                        }
                                        else if (cond == K_OR || cond == K_NULL) {
											(*records_found)++;
                                            memcpy(selection[sel_index++], records[i], (size_t) record_size);
                                            break;
                                        }
									}
									break;
								case S_GREATER:
									if (cond_value < buf_value)
									{
                                        if (cond == K_AND) {
                                            continue;
                                        }
                                        else if (cond == K_OR || cond == K_NULL) {
											(*records_found)++;
                                            memcpy(selection[sel_index++], records[i], (size_t) record_size);
                                            break;
                                        }

									}
									break;
								default:
									break;
							}

						}
						else
						{
							printf("Invalid statement after WHERE\n");
							return NULL;
						}
					}

				}


			}
			cur = cur->next;
		}


	}
	if (!*records_found) return NULL;
	return selection;

}

//deletes all records that meet a condition
int delete_records(token_list* tok, cd_entry* columns,  int num_cols, table_file_header* header, char** records)
{
	int num_deleted = 0;
	token_list* cur = tok;
	if (tok == NULL)//no condition, delete all records from table
	{
		for (int i = 0; i < header->num_records; i++)
		{
			memset((void*) records[i], '\0', (size_t) header->record_size);
		}

		header->file_size -= header->record_size * header->num_records;
		header->num_records = 0;
	}
	else
	{
		token_value cond = K_NULL;
		if (cur->next->tok_value == K_OR || cur->next->tok_value == K_AND)
		{
			cond = (token_value) cur->next->tok_value;
		}
		cur = cur->next; //skip the 'next' keyword
		for (int i = 0; i < header->num_records; i++)
		{

			while(cur->tok_class != terminator)
			{
				if (cur->tok_value == IDENT)
				{
					for (int j = 0; j < num_cols; j++)
					{
						if (columns[j].col_type == T_CHAR || columns[j].col_type ==  T_VARCHAR)
						{
							int rec_index = 0;

							for (int k = 0; k < j; k++)
							{
								rec_index += columns[k].col_len;
								if (columns[k].col_type == T_CHAR || columns[j].col_type == T_VARCHAR) rec_index++;
							}
							int size = 0;
							size = (int) records[i][rec_index++];
							char buffer[size];
							memcpy(buffer, &records[i][rec_index], (size_t) size);
							cur = cur->next;
							if (cur->tok_value != S_EQUAL)
							{
								return NULL;
							}
							cur = cur->next; // move to condition value
							if (strncmp(toupper(buffer), toupper(cur->tok_string), (size_t) size) == 0)
							{

								if (cond == K_OR || cond == K_NULL )//its true, condition has been satisfied
								{

									(num_deleted)++;
									memset(&records[i][0],'\0',1);//mark this for deletion
									break;
								}
								else
								{

									continue;//and
								}
							}
							else if (cond == K_AND)//false with an AND
							{

								break;

							}
						}
						else if (columns[j].col_type == T_INT)//we have an integer column
						{
							char buffer[4];
							int rec_index = 0;
							int k = 0;
							for (k; k < j; k++)
							{
								rec_index += columns[k].col_len;
								if (columns[j].col_type == T_CHAR || columns[j].col_type == T_VARCHAR) rec_index++;
							}
							//rec_index+= k;
							memcpy(buffer, &records[i][rec_index], sizeof(int));
							int buf_value = (int) *buffer;


							cur = cur->next;
							int cond_value = atoi(cur->next->tok_string);
							switch(cur->tok_value)
							{

								case S_EQUAL:

									if (cond_value == buf_value)
									{
										if (cond == K_AND)
										{
											continue;
										}
										else if (cond == K_OR || cond == K_NULL) {
											num_deleted++;
                                            memset(&records[i][0],'\0',1);//mark this for deletion
											break;
										}
									}
									break;
								case S_LESS:
									if (cond_value > buf_value)
									{
										if (cond == K_AND)
										{
											continue;
										}
										else if (cond == K_OR || cond == K_NULL) {
											num_deleted++;
                                            memset(&records[i][0],'\0',1);//mark this for deletion
											break;
										}
									}
									break;
								case S_GREATER:
									if (cond_value < buf_value)
									{
										if (cond == K_AND) {
											continue;
										}
										else if (cond == K_OR || cond == K_NULL) {
											num_deleted++;
                                            memset(&records[i][0],'\0',1);//mark this for deletion
											break;
										}

									}
									break;
								default:
									break;
							}

						}
						else
						{
							printf("Invalid statement after WHERE\n");
							return NULL;
						}
					}
				}
				cur = cur->next;
			}

		}
	}

	for (int i = 0; i < num_deleted; i++)
    {
        if (records[i][0] == '\0')
            memcpy((void*) records[i], (void*) records[(header->num_records) - 1], size_t (header->record_size));
        header->num_records--;
    }
    header->file_size -= header->record_size * num_deleted;
    return num_deleted;

}
//converts a string to all uppercase
//helper function for selection, for comparing
char* toupper(const char* string)
{
	if (string == NULL) return NULL;
	char* result = (char*) calloc(1, strlen(string));
	memcpy(result, string, strlen(string));
	int i = 0;
	while (result[i] != '\0')
	{
		result[i] = (char) toupper(result[i]);
		i++;
	}

	return result;
}
//returns a pointer to a columns entries for a projection operation
// also stores the new record size and number of columns for the projection
cd_entry* get_projected_columns(cd_entry* columns, token_list* tok, int num_columns, int* p_record_size, int* num_pcols)
{
	token_list* cur = tok;
	token_list* col_names = tok;

	while (col_names->tok_value == IDENT) {
		(*num_pcols)++;
		col_names = col_names->next;
	}
	cd_entry* pcolumns = (cd_entry*) calloc(*num_pcols, sizeof(cd_entry) );
	int index = 0;
	int pindex = 0;
	while (index < num_columns)
	{
		if (strcmp(toupper(cur->tok_string), toupper(columns[index].col_name)) == 0)
		{
            memcpy((void*) &pcolumns[pindex++], (void*) &columns[index], sizeof(cd_entry));
		}
		index++;
	}
	*p_record_size = get_record_size(pcolumns, *num_pcols);

	return pcolumns;
}
//returns the records for the projection
char** project_records(cd_entry* cols, cd_entry* proj_cols, int num_cols, int num_pcols, char** records, int num_records, int p_record_size)
{



	char** projection = (char**) calloc((size_t) num_records, (size_t) p_record_size);
	for (int i = 0; i < num_records; i++)
	{
		projection[i] = (char*) calloc(1, (size_t) p_record_size);
	}
	int cur_rec = 0;
	int rec_index = 0;
	int proj_index = 0;
	while(cur_rec < num_records)
	{
		for (int i = 0; i < num_pcols; i++)
		{
			for (int j = 0; j < num_cols; j++)
			{
				if (cols[j].col_id == proj_cols[i].col_id)
				{
					memcpy(&projection[cur_rec][proj_index], &records[cur_rec][rec_index], (size_t) cols[j].col_len );
					if (cols[j].col_type == T_CHAR || cols[j].col_type == T_VARCHAR) proj_index += cols[j].col_len + 1;
					else proj_index += sizeof(int);
				}
				if (cols[j].col_type == T_CHAR || cols[j].col_type == T_VARCHAR) rec_index += cols[j].col_len + 1;
				else rec_index += sizeof(int);

			}

		}
		rec_index = 0;
		proj_index = 0;
		cur_rec++;
	}

	return projection;
}
//returns the record size of the new projection
int get_record_size(cd_entry* columns, int num_cols)
{
    int projection_size = 0;
    for (int i = 0; i < num_cols; i++ )
    {
        if (columns[i].col_type == T_CHAR || columns[i].col_type == T_VARCHAR)
            projection_size+= columns[i].col_len;
        else
            projection_size += sizeof(int);

    }

    return projection_size;
}
//prints records to stdout in the specified format
void print_records(char** records, cd_entry* cols,  int record_size, int num_records, int num_cols)
{
	//print header
	for (int i = 0; i < num_cols - 1; i++)
	{
		printf("+----------------");
	}

	printf("+----------------+\n");

	for (int i = 0; i < num_cols; i++)
	{

		printf("|% 16s", cols[i].col_name);

	}
	printf("|\n");
	for (int i = 0; i < num_cols - 1; i++)
	{
		printf("+----------------");
	}

	printf("+----------------+\n");
	char buffer[record_size];

	//  fseek(fp, header->record_offset, SEEK_SET);
	for (int i = 0; i < num_records; i++)
	{

		memcpy(buffer, records[i], record_size);

		int index = 0;
		for (int j = 0; j < num_cols; j++)
		{
			if (cols[j].col_type == T_INT)
			{

				unsigned char int_buffer[sizeof(int)];
				int k = 0;
				memcpy((void*) int_buffer, (void*) &records[i][index], sizeof(int));
				index += 4;
				int num = 0;

				num = (int) *int_buffer;
				printf("|% 16d", num);

			}
			else
			{
				int length = (int) buffer[index++];
				if (length <= 0)
				{
					printf("|%- 16s", "(null)" );
					continue;
				}
				char val_buff[length + 1];
				int k = 0;


				for (k; k < length; k++)
				{
					val_buff[k] = buffer[index + k];
				}
				index += (cols[j].col_len - k) + k;

				val_buff[length] = '\0';


				printf("|%- 16s", val_buff );
			}

		}

		printf("|\n");
	}
	for (int i = 0; i < num_cols - 1; i++)
	{
		printf("+----------------");
	}

	printf("+----------------+\n");

}


int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
//	struct _stat file_stat;
	struct stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
//		_fstat(_fileno(fhandle), &file_stat);
		fstat(fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}