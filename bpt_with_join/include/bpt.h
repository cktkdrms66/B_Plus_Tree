#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#define DEFAULT_ORDER 32
#define BRANCH_ORDER 249
#define BUFFSIZE 4096
#define HEADER_RESERVED (BUFFSIZE - 24)
#define FREE_RESERVED (BUFFSIZE - 8)
#define FDMAX 10


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


//Buffer struct
typedef struct buffer
{
	page_t frame;
	int table_id;
	pagenum_t pageNum;
	char isDirty;
	char isPinned;
	struct buffer* prev;
	struct buffer* next;
} buffer;

typedef struct LRU
{
	buffer* next;
	buffer* last;
} LRU;

//Global value
extern int order;
extern int branch_order;
extern int fd;
extern page_t* headerPage;
extern int table_id;
extern buffer* bufArr;


//join fun
int join_table(int table_id_1, int table_id_2, char* pathname);
pagenum_t find_first_leaf(int table_id, buffer* headerBuf);


//Master fun
int open_table(char *pathname);
int db_insert(int table_id, int64_t key, char * value);
int db_find(int table_id, int64_t key, char * ret_val);
int db_delete(int table_id, int64_t key);
int close_table(int table_id);
int shutdown_db(void);
int init_db(int buf_num);


//File IO fun
void file_read_page(int fd, pagenum_t pagenum, page_t* dest);
void file_write_page(int fd, pagenum_t pagenum, const page_t* src);
int check_file_size(int fd);

//buffer help fun
pagenum_t buf_alloc_page(int table_id);
void buf_free_page(int table_id, pagenum_t pagenum);
buffer* buf_read_page(int table_id, pagenum_t pagenum);
void buf_write_page(int table_id, pagenum_t pagenum, const buffer* buf_src);



//Make fun
record * make_record(int64_t key, char* value);
page_t* make_leaf();
page_t* make_node();



//Insert help fun
pagenum_t find_leaf(int table_id, int64_t key);
int start_new_tree(int table_id, record * newRecord);
void insert_into_leaf(int table_id, buffer* leafBuf, int64_t key, record * pointer);
void insert_into_leaf_after_splitting(int table_id, buffer* leafBuf, int64_t key, record * pointer);
void insert_into_parent(int table_id, buffer* leftBuf, buffer* rightBuf, int64_t key);
void insert_into_new_root(int table_id, buffer* leftBuf, buffer* rightBuf, int64_t k_prime);
void insert_into_node(int table_id, buffer* nBuf, int right_index, int64_t key, buffer* right);
void insert_into_node_after_splitting(int table_id, buffer* old_nodeBuf, int right_index, int64_t key, buffer* rightBuf);

int cut(int length);
int get_right_index(buffer* parentBuf, pagenum_t leftNum);



//Delete help fun
void delete_entry(int table_id, buffer* c, int64_t key);
void remove_entry_from_node(int table_id, buffer* nBuf, int64_t key);
void coalesce_nodes(int table_id, buffer* n, buffer* neighbor, int neighbor_index, int64_t k_prime);
void adjust_root(int table_id, buffer* c);
void redistribute_nodes(int table_id, buffer* n, buffer* neighbor, int neighbor_index, int k_prime_index, int64_t k_prime);
int get_neighbor_index(int table_id, buffer* n);




//프린트
void print_page(int table_id, pagenum_t pageNum);
void print_buf(int table_id);
void print_LRU();
