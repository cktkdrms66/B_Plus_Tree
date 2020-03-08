#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#define DEFAULT_ORDER 32
#define BRANCH_ORDER 249
#define BUFFSIZE 4096
#define HEADER_RESERVED (BUFFSIZE - 24)
#define FREE_RESERVED (BUFFSIZE - 8)


typedef uint64_t pagenum_t;
typedef struct record
{
	int64_t key;
	char value[120];
} record;

typedef struct pageNumAndKey
{
	int64_t key;
	pagenum_t pageNumber;
} pageNumAndKey;

typedef struct HeaderPage
{
	
	pagenum_t freePageNum;
	pagenum_t rootPageNum;
	pagenum_t numOfPage;
	char reserved[HEADER_RESERVED];
}HeaderPage;

typedef struct FreePage
{
	pagenum_t nextFreePageNum;
	char reserved[FREE_RESERVED];

} FreePage;

typedef struct NodePage
{
	pagenum_t parentPageNum;
	int isLeaf;
	int NumOfKey;
	char reserved[104];

	union
	{
		
		pagenum_t rightPageNum;
		pagenum_t firstPageNum;
	};
	
	union
	{
		record recordArr[31];
		pageNumAndKey pageArr[248];
	};

} NodePage;


//페이지 구조체
typedef struct page_t
{
	union
	{
		HeaderPage headerPage;
		FreePage freePage;
		NodePage nodePage;
	};
} page_t;

typedef struct node
{
	off_t page;
	struct node* next;
} node;



//Global value
extern int order;
extern int branch_order;
extern int fd;
extern page_t* headerPage;
extern node* queue;


//Master fun
int open_table(char *pathname);
int db_insert(int64_t key, char * value);
int db_find(int64_t key, char * ret_val);
int db_delete(int64_t key);
void close_table();

//File IO fun
pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t* dest);
void file_write_page(pagenum_t pagenum, const page_t* src);



//Make fun
record * make_record(int64_t key, char* value);
page_t* make_leaf();
page_t* make_node();



//Insert help fun
pagenum_t find_leaf(int64_t key);
int start_new_tree(record * newRecord);
void insert_into_leaf(pagenum_t insertNum, page_t * leaf, int64_t key, record * pointer);
void insert_into_parent(page_t * left, page_t * right, int64_t key, pagenum_t leftNum, pagenum_t rightNum);
void insert_into_leaf_after_splitting(pagenum_t insertNum, page_t * leaf, int64_t key, record * pointer);
void insert_into_new_root(page_t * left, page_t * right, int64_t k_prime, pagenum_t leftNum, pagenum_t rightNum);
void insert_into_node(page_t * n, int right_index, int64_t key, page_t * right, pagenum_t rightNum);
void insert_into_node_after_splitting(page_t * old_node, int right_index, int64_t key, page_t * right, pagenum_t rightNum);
int cut(int length);
int get_right_index(page_t * parent, pagenum_t leftNum);

//Delete help fun
void delete_entry(pagenum_t cNum, page_t* c, int64_t key);
void remove_entry_from_node(pagenum_t nNum, page_t* n, int64_t key);
void adjust_root(pagenum_t cNum, page_t* c);
void coalesce_nodes(pagenum_t nNum, pagenum_t neighborNum, page_t * n, page_t * neighbor, int neighbor_index, int64_t k_prime);
void redistribute_nodes(pagenum_t nNum, pagenum_t neighborNum, page_t * n, page_t * neighbor, int neighbor_index, int k_prime_index, int64_t k_prime);
int get_neighbor_index(pagenum_t nNum, page_t* n);


//프린트
void print_page(pagenum_t pageNum);
