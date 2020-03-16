#include "bpt.h"
#include <stdlib.h>
#include<stdio.h>

// MAIN

int main(int argc, char ** argv) {

	char * input_file = "a.dat";
	page_t * root;
	int64_t input, range2;
	char instruction;
	char license_part;
	char * temp;
	temp = malloc(sizeof(char) * 120);
	root = NULL;
	int n1;
	
	open_table(input_file);
	
	
	int n2;
	printf("> ");
	fflush(stdin);
	temp = (char*)malloc(120);
	printf("------DBMS Disk Based B+Tree-----\n");
	printf("(1) i key value - insert key into tree\n");
	printf("(2) d key - delete key from tree if the key exists\n");
	printf("(3) f key - find the key that you want to find and print value the key has\n");
	printf("(4) p - print tree\n");
	printf("(5) q - quit program\n"); 
	while (scanf("%c", &instruction) != EOF) {
			switch (instruction) {
		case 'd':
			scanf("%ld", &input);
			db_delete(input);
			print_tree();
			break;
		case 'i':
			strcpy(temp,"a");
			scanf("%d",&n2);
			for(int i=1;i<=n2;i++){	
			input = i;
			
			//scanf("%ld", &input);
			//scanf("%s", temp);
			db_insert(input, temp);
			}
			print_tree();
			break;
		case 'f':
			scanf("%ld", &input);
			if(db_find(input, temp) == -1) printf("there is no key that you want\n");
			else printf("%s is the value that you want to find\n", temp);
			break;
		case 'q':
			while (getchar() != (int)'\n');
			return 0;
		case 'p':
			print_tree();
			break;
			/* case 'r':
			scanf("%d %d", &input, &range2);
			if (input > range2) {
			int tmp = range2;
			range2 = input;
			input = tmp;
			}
			find_and_print_range(root, input, range2, instruction == 'p');
			break;
			case 'l':
			print_leaves(root);
			break;
			case 'q':
			while (getchar() != (int)'\n');
			return EXIT_SUCCESS;
			break;
			case 't':
			print_tree(root);
			break;
			case 'v':
			verbose_output = !verbose_output;
			break;
			case 'x':
			if (root)
			root = destroy_tree(root);
			print_tree(root);
			break;*/
		default:
			//usage_2();
			fflush(stdin);
			break;
		}
		printf("\n\n\n\n------DBMS Disk Based B+Tree-----\n");
		printf("(1) i key value - insert key into tree\n");
		printf("(2) d key - delete key from tree if the key exists\n");
		printf("(3) f key - find the key that you want to find and print value the key has\n");
		printf("(4) p - print tree\n");
		printf("(5) q - quit program\n"); 
		while (getchar() != (int)'\n');
		printf("> ");
	}
	printf("\n");
	return EXIT_SUCCESS;
}
