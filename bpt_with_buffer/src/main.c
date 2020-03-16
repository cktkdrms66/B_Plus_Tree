#include "bpt.h"


int main() {
	
	int random;
	int who;
	int num;
	char* input_file = "chasanggod.dat";
	char* input_file2 = "chasanggod";
	char* input_file3 = "chasanggod2";
	char* input_file4 = "chasanggod3";
	char* input_file5 = "chasanggod4";
	char* input_file6 = "chasanggod5";
	char* input_file7 = "chasanggod6";
	char* input_file8 = "chasanggod7";
	char* input_file9 = "chasanggod8";
	
	char input2[120];
	char str;

	//OPEN
	init_db(BUFMAX);
	open_table(input_file);
	open_table(input_file2);	
	open_table(input_file3);
	open_table(input_file4);
	open_table(input_file5);
	open_table(input_file6);
	open_table(input_file7);
	open_table(input_file8);
	open_table(input_file9);
	//INSERT
	for(int i = 0; i < INPUT; i++)
	{
		who = rand() % 2;
		num = rand() % 4;
		for(int j = 0; j < num; j++)
		{
			str = 65 + rand() % 30;
					
			input2[j] = str;
		}
		random = rand() % 3400;
		
		printf("insert %d\n", random);
		if(who == 0) db_insert(1, random, input2);
		else db_insert(2, random, input2);
		

	}
	join_table(1,2, "result.txt");
	
	printf("\n\n\n");
	shutdown_db();

	init_db(BUFMAX);
	open_table(input_file);
	open_table(input_file2);	

	
	return EXIT_SUCCESS;
}
