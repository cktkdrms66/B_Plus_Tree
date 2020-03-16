#include "bpt.h"


int main() {
   int i = 0;
	char* input_file2 = "chasanggod";
	char* input_file3 = "chasanggod2";
	char* input_file4 = "chasanggod3";

	init_db(1000);
	open_table(input_file2);	
	open_table(input_file3);
	open_table(input_file4);
   while(1) {
      char choice[100]; 
	
      scanf("%s",choice);


      if(strcmp(choice, "init") == 0) {
         
         int buffer_num;
         scanf("%d", &buffer_num);
      
         init_db(buffer_num);

      }

      else if(strcmp(choice, "1") == 0) {
	printf("insert\n");
         int table_id = 0;
         int64_t key = 0;
         char value[120] = {0, };

         scanf("%d %ld %s", &table_id, &key, value);

         if(db_insert(table_id, key, value) == -1)
            printf("insert fail\n");
         else 	
            printf("insert %d success\n", i);
         i++;

      }

      else if(strcmp(choice, "3") == 0) {
         int table_id = 0;
         int64_t key = 0;

         scanf("%d %ld", &table_id, &key);

         if(db_delete(table_id, key) == -1)
            printf("delete fail\n");
         else
            printf("delete success\n");
      }


      else if(strcmp(choice, "5") == 0) {
         int table_id1 = 0;
         int table_id2 = 0;
         char result[200];

         scanf("%d %d %s", &table_id1, &table_id2, result);

         if(join_table(table_id1, table_id2, result) == -1) printf("fail\n");
	else printf("susses\n");
      
      }

      else if(strcmp(choice, "pp") == 0) {
         pagenum_t page_num;
         
         int table_id;   
      
         scanf("%d %ld", &table_id, &page_num);

         print_page(table_id, page_num);
      }


      else if(strcmp(choice, "c") == 0) {
         int table_id = 0;

         scanf("%d", &table_id);

         close_table(table_id);
      }

      else if(strcmp(choice, "quit") == 0)
         break;


   }

   shutdown_db();

   return 0;
}
