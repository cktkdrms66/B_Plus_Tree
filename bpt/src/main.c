#include "bpt.h"
#define INPUT 10000
#define PAGENUM 1000
#include <time.h>


int main() {
	
	srand(time(NULL));
	int random;


	char* input_file = "new_file.dat";
	char input2[120];
	input2[0] = 'a';


	//OPEN
	open_table(input_file);

	

	//INSERT
	for(int i = 0; i < INPUT; i++)
	{
		random = rand() % 100;
		printf("insert %d\n", i);
		db_insert(i, input2);		
	}
	
	//DELETE
	for(int i = 0; i < INPUT; i++)
	{
		random = rand() % 100;
		if(db_find(i, input2) == 0) printf("find %d\n", i);	
	}
	 	
	close_table();	    	
	return EXIT_SUCCESS;
}
