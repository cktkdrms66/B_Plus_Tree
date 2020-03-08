#include "bpt.h"
int order = DEFAULT_ORDER;
int branch_order = BRANCH_ORDER;
char* charArr[FDMAX];
int fdArr[FDMAX];
buffer* bufArr;
LRU LRUheader;
int BUFMAX;
//file IO function
void file_read_page(int fd, pagenum_t pagenum, page_t* dest)
{
	lseek(fd, pagenum * BUFFSIZE, SEEK_SET);
	read(fd, dest, BUFFSIZE);
}

void file_write_page(int fd, pagenum_t pagenum, const page_t* src)
{
	lseek(fd, pagenum * BUFFSIZE, SEEK_SET);
	write(fd, src, BUFFSIZE);
}

int check_file_size(int fd)
{
	return lseek(fd, 0, SEEK_END);
}




//join
pagenum_t find_first_leaf(int table_id, buffer* headerBuf)
{
	int cNum;
	buffer* cBuf; 	 

	cNum = headerBuf->frame.headerPage.rootPageNum;
	
	if ( cNum == 0 )
	{
		return 0;
	}
	cBuf = buf_read_page(table_id, cNum);
	
	while ( cBuf->frame.nodePage.isLeaf != 1)
	{
		cNum = cBuf->frame.nodePage.firstPageNum;
		cBuf->isPinned--;
		cBuf = buf_read_page(table_id, cNum);	
	}
	cBuf->isPinned--;
	return cNum;



}
int join_table(int table_id_1, int table_id_2, char* pathname)
{
	
	int fd_1, fd_2, i, j;
	pagenum_t leafNum_1, leafNum_2;
	buffer* headerBuf_1;
	buffer* headerBuf_2;
	buffer* leafBuf_1;
	buffer* leafBuf_2;
	FILE* file = NULL;

	if((file = fopen( pathname, "w")) == NULL) return -1;

	fd_1 = fdArr[table_id_1 - 1];
	fd_2 = fdArr[table_id_2 - 1];

		
	headerBuf_1 = buf_read_page(table_id_1, 0);
	headerBuf_2 = buf_read_page(table_id_2, 0);
	
	leafNum_1 = find_first_leaf(table_id_1, headerBuf_1);
	leafNum_2 = find_first_leaf(table_id_2, headerBuf_2);

	if(leafNum_1 == 0 || leafNum_2 == 0)
	{
		headerBuf_1->isPinned--;
		headerBuf_2->isPinned--;
		return -1;
	}


	leafBuf_1 = buf_read_page(table_id_1, leafNum_1);
	leafBuf_2 = buf_read_page(table_id_2, leafNum_2);

	headerBuf_1->isPinned--;
	headerBuf_2->isPinned--;

		
	i = 0;
	j = 0;
	while(1)
	{
		
		if(leafBuf_1->frame.nodePage.recordArr[i].key == leafBuf_2->frame.nodePage.recordArr[j].key)
		{
			fprintf(file, "%ld,%s,%ld,%s\n", leafBuf_1->frame.nodePage.recordArr[i].key, 
				leafBuf_1->frame.nodePage.recordArr[i].value, 
				leafBuf_2->frame.nodePage.recordArr[j].key, 
				leafBuf_2->frame.nodePage.recordArr[j].value);
		
			i++;
			j++;
		}
		else if(leafBuf_1->frame.nodePage.recordArr[i].key < leafBuf_2->frame.nodePage.recordArr[j].key) i++;
	
			
		else if(leafBuf_1->frame.nodePage.recordArr[i].key > leafBuf_2->frame.nodePage.recordArr[j].key) j++;


		 

		if(i == leafBuf_1->frame.nodePage.NumOfKey)
		{
				leafNum_1 = leafBuf_1->frame.nodePage.rightPageNum;
				if(leafNum_1 != 0)
				{				
					leafBuf_1->isPinned--;				
					leafBuf_1 = buf_read_page(table_id_1, leafNum_1);
					i = 0;
				}
				else
				{
					leafBuf_1->isPinned--;
					leafBuf_2->isPinned--;
					fclose(file);
					return 0;
				}		
		}
		if(j == leafBuf_2->frame.nodePage.NumOfKey)
		{
				leafNum_2 = leafBuf_2->frame.nodePage.rightPageNum;
				if(leafNum_2 != 0)
				{
					leafBuf_2->isPinned--;
					leafBuf_2 = buf_read_page(table_id_2, leafNum_2);
					j = 0;
				}
				else
				{
					leafBuf_1->isPinned--;
					leafBuf_2->isPinned--;
					fclose(file);
					return 0;
				}		
		}
		

	}		

}
//Buffer function
pagenum_t buf_alloc_page(int table_id)
{
	buffer* buf;
	buffer* firstBuf;
	buffer* secondBuf;
 
	buf = buf_read_page(table_id, 0);

	pagenum_t firstFreePageNum, secondFreePageNum;
	
	//case1 : there is no freepage 
	if ( buf->frame.headerPage.freePageNum == 0 )
	{	
		buf->frame.headerPage.numOfPage++;
		buf->isPinned--;
		return buf->frame.headerPage.numOfPage -1;
	}
	//case2 : freePage exists
	firstFreePageNum = buf->frame.headerPage.freePageNum;
	firstBuf = buf_read_page(table_id, firstFreePageNum);
	
	secondFreePageNum = firstBuf->frame.freePage.nextFreePageNum;
	
	buf->frame.headerPage.freePageNum = secondFreePageNum;
	buf->isPinned--;
	firstBuf->isPinned--;
	return firstFreePageNum;

}


void buf_free_page(int table_id, pagenum_t pagenum)
{
	pagenum_t firstFreePageNum;
	buffer* buf; 
	buffer* currBuf;
	buf = buf_read_page(table_id, 0);

	firstFreePageNum = buf->frame.headerPage.freePageNum;
	currBuf = buf_read_page(table_id, pagenum);
	currBuf->frame.freePage.nextFreePageNum = firstFreePageNum;
	currBuf->frame.nodePage.NumOfKey = 0;
	
	buf->frame.headerPage.freePageNum = pagenum;

	buf->isDirty = 1;
	currBuf->isDirty = 1;
	buf->isPinned--;
	currBuf->isPinned--;
}



buffer* buf_read_page(int table_id, pagenum_t pagenum)
{
	//printf("buf_read_page\n");
	
	page_t dest;
	int i;
	int index = table_id - 1;	
	buffer* currBuf; 
	
	
	for(i = 0; i < BUFMAX; i++)
	{
		if(bufArr[i].table_id == table_id && bufArr[i].pageNum == pagenum)
		{
			bufArr[i].isPinned++;
			if(LRUheader.last == &bufArr[i]) return &bufArr[i];

			if(LRUheader.next == &bufArr[i])
			{
				LRUheader.next = bufArr[i].next;
				bufArr[i].next->prev = NULL;
			}
			else
			{
				bufArr[i].prev->next = bufArr[i].next;
				bufArr[i].next->prev = bufArr[i].prev;
			}	
			bufArr[i].prev = LRUheader.last;
			bufArr[i].next = NULL;
			
			LRUheader.last->next = &bufArr[i];
			LRUheader.last = &bufArr[i];
			return &bufArr[i];	
		}
	}
	

	file_read_page(fdArr[index], pagenum, &dest);
	
	if(LRUheader.next == NULL)
	{
		bufArr[0].frame = dest;
		bufArr[0].table_id = table_id;
		bufArr[0].pageNum = pagenum;
		LRUheader.next = &bufArr[0];
		LRUheader.last = &bufArr[0];
		bufArr[0].isPinned++;	
		return &bufArr[0];
	}
	
	
	for(i = 0; i < BUFMAX; i++)
	{
		if(bufArr[i].table_id == 0)
		{
			bufArr[i].pageNum = pagenum;
			bufArr[i].frame = dest;
			bufArr[i].table_id = table_id;
			bufArr[i].prev = LRUheader.last;
			LRUheader.last->next = &bufArr[i];
			LRUheader.last = &bufArr[i];
			bufArr[i].isPinned++;
			return &bufArr[i];
		}
	}

	
	//buffer max max!!
	if(i == BUFMAX)
	{
		currBuf = LRUheader.next;
		while ( currBuf != NULL )
		{
			if (!currBuf->isPinned)
			{
				file_write_page(fdArr[currBuf->table_id -1], currBuf->pageNum, &(currBuf->frame));
				file_read_page(fdArr[index], pagenum, &(currBuf->frame));
				currBuf->isDirty = 1;
				currBuf->isPinned++;
				currBuf->pageNum = pagenum;
				currBuf->table_id = table_id;
				if(LRUheader.last == currBuf) return currBuf;
				if(LRUheader.next == currBuf)
				{
					LRUheader.next = currBuf->next;
					currBuf->next->prev = NULL;
				}
				else
				{
					currBuf->prev->next = currBuf->next;
					currBuf->next->prev = currBuf->prev;
				}			
				currBuf->prev = LRUheader.last;
				currBuf->next = NULL;
				LRUheader.last->next = currBuf;
				LRUheader.last = currBuf;
				return currBuf;
				
			}
			currBuf = currBuf->next;
		}
		
		printf("ALL Pinned!\n");
	}

}

void buf_write_page(int table_id, pagenum_t pagenum, const buffer* buf_src)
{
	int index = table_id - 1;

	file_write_page(fdArr[index], pagenum, &(buf_src->frame));	
}

//Master function
int open_table(char* pathname)
{
	
	int i,fd, index;
	int table_id = 0;
	page_t headerPage;
	buffer headerBuffer;
	buffer* currBuf;
	buffer* buf_dest;

	mode_t mode = S_IRUSR | S_IWUSR| S_IRGRP| S_IWGRP| S_IROTH| S_IWOTH;
		
	fd = open(pathname, O_RDWR | O_CREAT | O_EXCL| O_SYNC, mode);
	
			
	if(fd == -1)
	{
		fd = open(pathname, O_RDWR | O_SYNC);
	}
			
	for(i = 0; i < FDMAX; i++)
	{
		if(charArr[i] == NULL) continue;
		if ( strcmp(charArr[i], pathname) == 0 )
		{
			table_id = i + 1;
			index = i;
			break;
		}
	}


	if ( table_id == 0 )
	{
		for ( i = 0; i < FDMAX; i++ )
		{
			if ( charArr[i] == NULL )
			{
				table_id = i + 1;
				index = i;
				charArr[i] = pathname;
				break;
			}
		}
		if( i == FDMAX )
		{
			return -1;
		}
	}

	fdArr[index] = fd;
	if(check_file_size(fd) == 0)
	{
		
		//make headerpage
		headerPage.headerPage.numOfPage = 1;
		headerPage.headerPage.rootPageNum = 0;
		headerPage.headerPage.freePageNum = 0;
		
		headerBuffer.frame = headerPage;
		headerBuffer.table_id = table_id;
		headerBuffer.pageNum = 0;
		headerBuffer.isDirty = 1;
		headerBuffer.prev = NULL;
		headerBuffer.next = NULL;
		if(LRUheader.next == NULL)
		{
			bufArr[0] = headerBuffer;
			
			LRUheader.next = &bufArr[0];
			LRUheader.last = &bufArr[0];
			return table_id;
		}
	
	
		for(i = 0; i < BUFMAX; i++)
		{
			if(bufArr[i].table_id == 0)
			{
			
				bufArr[i] = headerBuffer;
				bufArr[i].prev = LRUheader.last;
				LRUheader.last->next = &bufArr[i];
				LRUheader.last = &bufArr[i];
				return table_id;
			}
		}

		if(i == BUFMAX)
		{
			currBuf = LRUheader.next;
			while ( currBuf != NULL )
			{
				if ( !currBuf->isPinned)
				{
					file_write_page(fdArr[index], currBuf->pageNum, &(currBuf->frame));
					file_read_page(fdArr[index], 0, &(currBuf->frame));
					currBuf->isDirty = 0;
					currBuf->table_id = table_id;
					currBuf->prev->next = currBuf->next;
					currBuf->next->prev = currBuf->prev;
					currBuf->prev = LRUheader.last;
					currBuf->next = NULL;
					LRUheader.last = currBuf;
					
				}
				currBuf = currBuf->next;
			}
		}
	}

	else
	{
		buf_dest = buf_read_page(table_id, 0);
		buf_dest->isPinned--;
	}
	
	return table_id;
}



int init_db(int num_buf)
{

	bufArr = (buffer*)malloc(sizeof(buffer) * num_buf);
	if(bufArr == NULL) return -1;
	BUFMAX = num_buf;
	for(int i = 0; i < num_buf; i++)
	{
		bufArr[i].table_id = 0;
		bufArr[i].pageNum = 0;
		bufArr[i].isDirty = 0;
		bufArr[i].isPinned = 0;
		bufArr[i].next = NULL;
		bufArr[i].prev = NULL;
	}
	LRUheader.next = NULL;
	LRUheader.last = NULL;
	for(int i = 0; i < FDMAX; i++)
	{
		fdArr[i] = 0;
		charArr[i] = NULL;
	}
	return 0;
}
	
int close_table(int table_id)
{
	buffer* currBuf;
	buffer* nextBuf;	
	int index = table_id - 1;
	int count = 0;

	if(fdArr[index] == 0) return -1;

	for(int i = 0; i < BUFMAX; i++)
	{
		if(bufArr[i].table_id == table_id && bufArr[i].isPinned == 0)
		{
			if(bufArr[i].isDirty != 0) file_write_page(fdArr[index], bufArr[i].pageNum, &(bufArr[i].frame));
			bufArr[i].table_id = 0;
			bufArr[i].isPinned = 0;
			bufArr[i].isDirty = 0;
			count++;			
		}

	}
	
	if(count != 0)
	{
		currBuf = LRUheader.next;
		while(currBuf->next != NULL)
		{
			nextBuf = currBuf->next;
			if(currBuf->table_id == 0)
			{
				if(LRUheader.next == currBuf)
				{
					LRUheader.next = nextBuf;
				}
				else
				{
					currBuf->prev->next = currBuf->next;
					currBuf->next->prev = currBuf->prev;
				}
				currBuf = NULL;
			}
			currBuf = nextBuf;
		}
		if(currBuf->table_id == 0)	
		{
			if(LRUheader.next == currBuf)
			{
				LRUheader.next = NULL;
				LRUheader.last = NULL;
			}
			else
			{
				currBuf->prev->next = currBuf->next;
			}
		}	

		currBuf = LRUheader.next;	
		if(currBuf != NULL)	
		{
			while(currBuf->next != NULL)
			{
				currBuf = currBuf->next;
			}
			LRUheader.last = currBuf;	
		}

	}
	close(fdArr[index]);
	fdArr[index] = 0;
	return 0;

}

int shutdown_db()
{
	
	for(int i = 0; i < FDMAX; i++)
	{
		if(fdArr[i] != 0)
		{			
			close_table(i + 1);
		}
	}

	free(bufArr);
	return 0;
}

int db_find(int table_id, int64_t key, char * ret_val)
{
	int i = 0;
	buffer* cBuf;
	pagenum_t cNum = find_leaf(table_id, key);
	
	if ( cNum == 0 ){
		return -1;
	}
	cBuf = buf_read_page(table_id, cNum);
	
	for ( i = 0; i < cBuf->frame.nodePage.NumOfKey; i++ )
		if ( cBuf->frame.nodePage.recordArr[i].key == key ) break;
	
	if ( i == cBuf->frame.nodePage.NumOfKey )
	{	
		cBuf->isPinned--;
		return -1;
	}
	else
	{
		//레코드의 value를 ret_val에 담기
		for ( int j = 0; j < 120; j++ )
		{
			ret_val[j] = cBuf->frame.nodePage.recordArr[i].value[j];
		}
		cBuf->isPinned--;
		return 0;
	}
		
}

int db_insert(int table_id, int64_t key, char* value)
{

	record* newRecord;
	page_t* root;
	page_t* leaf;
	pagenum_t insertNum;
	pagenum_t leafNum;
	buffer* buf; 
	buffer* leafBuf;

	char ret_val[120];

	buf = buf_read_page(table_id, 0);
	
	//해당 키가 이미 존재한다면 
	if ( db_find(table_id, key, ret_val) == 0 )
	{
		buf->isPinned--;
		return -1;
	}
	newRecord = make_record(key, value);
	
	//루트페이지가 없을때
	if ( buf->frame.headerPage.rootPageNum == 0 )
	{
		buf->isPinned--;		
		return start_new_tree(table_id, newRecord);
	}
	insertNum = find_leaf(table_id, key);
	leafBuf = buf_read_page(table_id, insertNum);
		
	
	//리프노드에 빈자리가 있을 때
	if ( leafBuf->frame.nodePage.NumOfKey < order - 1 )
	{
		buf->isPinned--;
		insert_into_leaf(table_id, leafBuf, key, newRecord);
		return 0;
	}

	//리프노드에 빈자리가 없을 때
	buf->isPinned--;
	insert_into_leaf_after_splitting(table_id, leafBuf, key, newRecord);
	return 0;
}

record * make_record(int64_t key, char* value)
{
	record * new_record = (record *)malloc(sizeof(record));
	if ( new_record == NULL )
	{
		perror("Record creation.");
		exit(EXIT_FAILURE);
	}
	else
	{
		new_record->key = key;
		for ( int i = 0; i < 120; i++ )
		{
			new_record->value[i] = value[i];
		}
	}
	return new_record;
}


int start_new_tree(int table_id, record * newRecord)
{

	page_t* root = make_leaf();
	buffer* buf;
	buffer* rootBuf;
	pagenum_t rootNum = buf_alloc_page(table_id);
	
	buf = buf_read_page(table_id, 0);
	rootBuf = buf_read_page(table_id, 1);
	
	root->nodePage.recordArr[0].key = newRecord->key;
	for(int i = 0; i < 120; i++) root->nodePage.recordArr[0].value[i] = newRecord->value[i];
	root->nodePage.firstPageNum = 0;
	root->nodePage.parentPageNum = 0;
	root->nodePage.NumOfKey = 1;
	
	buf->frame.headerPage.rootPageNum = 1;
	rootBuf->frame = *root;

	buf->isDirty = 1;
	rootBuf->isDirty = 1;

	free(newRecord);
	free(root);

	buf->isPinned--;
	rootBuf->isPinned--;

	return 0;
}

pagenum_t find_leaf(int table_id, int64_t key)
{	
	
	int i;
	pagenum_t cNum;
	buffer* cBuf;

	//cBuf is headerPage
	cBuf = buf_read_page(table_id, 0);
	
	cNum = cBuf->frame.headerPage.rootPageNum;
	cBuf->isPinned--;
	
	if ( cNum == 0 )
	{
		return 0;
	}
	cBuf = buf_read_page(table_id, cNum);
	
	while ( cBuf->frame.nodePage.isLeaf != 1)
	{
		i = 0;
		while ( i < cBuf->frame.nodePage.NumOfKey )
		{
			if( key < cBuf->frame.nodePage.pageArr[0].key)
			{
				i = -1;
				break;
			}
			if( key >= cBuf->frame.nodePage.pageArr[i].key) i++;
			else break;
		}
		
		if ( i == -1 )
		{
			cNum = cBuf->frame.nodePage.firstPageNum;
			cBuf->isPinned--;
			cBuf = buf_read_page(table_id, cNum);
			
		}
		else
		{
			cNum = cBuf->frame.nodePage.pageArr[i -1].pageNumber;
			cBuf->isPinned--;
			cBuf = buf_read_page(table_id, cNum);
		}
		
	}
	cBuf->isPinned--;
	return cNum;
}

int cut(int length)
{
	if ( length % 2 == 0 )
		return length / 2;
	else
		return length / 2 + 1;
}
void copy_node(page_t node, buffer* buf)
{
	buf->frame = node;
}
page_t * make_node()
{
	page_t * new_node;
	new_node = (page_t*)malloc(sizeof(page_t));
	if ( new_node == NULL )
	{
		perror("Node creation.");
		exit(EXIT_FAILURE);
	}
	new_node->nodePage.isLeaf = false;
	new_node->nodePage.NumOfKey = 0;
	new_node->nodePage.parentPageNum = 0;
	new_node->nodePage.firstPageNum = 0;

	return new_node;
}


page_t * make_leaf(void)
{
	page_t * leaf = make_node();
	leaf->nodePage.isLeaf = true;
	return leaf;
}

void insert_into_leaf(int table_id, buffer* leafBuf, int64_t key, record * pointer)
{
	int i, insertion_point;
	
	insertion_point = 0;
	while (insertion_point < leafBuf->frame.nodePage.NumOfKey && leafBuf->frame.nodePage.recordArr[insertion_point].key < key )
		insertion_point++;

	for ( i = leafBuf->frame.nodePage.NumOfKey; i > insertion_point; i-- )
	{
		leafBuf->frame.nodePage.recordArr[i] = leafBuf->frame.nodePage.recordArr[i - 1];
	}
	leafBuf->frame.nodePage.recordArr[insertion_point].key = pointer->key;
	for ( int j = 0; j < 120; j++ )
		leafBuf->frame.nodePage.recordArr[insertion_point].value[j] = pointer->value[j];
	
	leafBuf->frame.nodePage.NumOfKey++;
	leafBuf->isPinned--;
	leafBuf->isDirty = 1;
	free(pointer);
}


void insert_into_leaf_after_splitting(int table_id, buffer* leafBuf, int64_t key, record * pointer)
{
	page_t* new_leaf;
	buffer* new_leafBuf;
	record temp_records[DEFAULT_ORDER];
	pagenum_t new_leafNum;
	int insertion_index, split, new_key, i, j;

	new_leaf = make_leaf();
	new_leafNum = buf_alloc_page(table_id);
	new_leafBuf = buf_read_page(table_id, new_leafNum);
	new_leafBuf->frame = *new_leaf;
	new_leafBuf->isDirty = 1;

	insertion_index = 0;
	while ( insertion_index < order - 1 && leafBuf->frame.nodePage.recordArr[insertion_index].key < key )
		insertion_index++;

	for ( i = 0, j = 0; i < leafBuf->frame.nodePage.NumOfKey; i++, j++ )
	{
		if ( j == insertion_index ) j++;
		temp_records[j] = leafBuf->frame.nodePage.recordArr[i];
	}
	temp_records[insertion_index] = *pointer;
	
	leafBuf->frame.nodePage.NumOfKey = 0;

	split = cut(order - 1);

	for ( i = 0; i < split; i++ )
	{
		leafBuf->frame.nodePage.recordArr[i] = temp_records[i];
		leafBuf->frame.nodePage.NumOfKey++;
	}

	for ( i = split, j = 0; i < order; i++, j++ )
	{
		new_leafBuf->frame.nodePage.recordArr[j] = temp_records[i];
		new_leafBuf->frame.nodePage.NumOfKey++;
	}
	
	new_leafBuf->frame.nodePage.rightPageNum = leafBuf->frame.nodePage.rightPageNum;
	leafBuf->frame.nodePage.rightPageNum = new_leafNum;
	
	for ( i = leafBuf->frame.nodePage.NumOfKey; i < order - 1; i++ )
	{
		leafBuf->frame.nodePage.recordArr[i].key = 0;
		for ( int k = 0; k < 120; k++ )
		{
			leafBuf->frame.nodePage.recordArr[i].value[k] = 0;
 		}
	}

	for ( i = leafBuf->frame.nodePage.NumOfKey; i < order - 1; i++ )
	{
		new_leafBuf->frame.nodePage.recordArr[i].key = 0;
		for ( int k = 0; k < 120; k++ )
		{
			leafBuf->frame.nodePage.recordArr[i].value[k] = 0;
		}
	}
	new_leafBuf->frame.nodePage.parentPageNum = leafBuf->frame.nodePage.parentPageNum;
	new_key = new_leafBuf->frame.nodePage.recordArr[0].key;

	free(new_leaf);
	free(pointer);
	insert_into_parent(table_id, leafBuf, new_leafBuf, new_key);
}


void insert_into_parent(int table_id, buffer* leftBuf, buffer* rightBuf, int64_t key)
{
	int right_index;
	buffer* parentBuf;
	pagenum_t parentNum;

	parentNum = leftBuf->frame.nodePage.parentPageNum;

	/* Case: new root. */
	if ( parentNum == 0 )
	{
		
		insert_into_new_root(table_id, leftBuf, rightBuf, key);
		return;
	}
	parentBuf = buf_read_page(table_id, parentNum);
	right_index = get_right_index(parentBuf, leftBuf->pageNum);
	if ( parentBuf->frame.nodePage.NumOfKey < branch_order - 1 )
	{
		leftBuf->isPinned--;
		insert_into_node(table_id, parentBuf, right_index, key, rightBuf);
		return;
	}
	
	leftBuf->isPinned--;
	insert_into_node_after_splitting(table_id, parentBuf, right_index, key, rightBuf);
}


void insert_into_new_root(int table_id, buffer* leftBuf, buffer* rightBuf, int64_t k_prime)
{
	buffer* headerBuf;
	buffer* rootBuf;
	page_t* root = make_node();
	pagenum_t rootNum;
	if ( leftBuf->frame.nodePage.isLeaf == 1 )
	{
		root->nodePage.firstPageNum = leftBuf->pageNum;
		root->nodePage.pageArr[0].key = k_prime;
		root->nodePage.pageArr[0].pageNumber = rightBuf->pageNum;
	}
	else
	{
		root->nodePage.firstPageNum = leftBuf->pageNum;
		root->nodePage.pageArr[0].key = k_prime;
		root->nodePage.pageArr[0].pageNumber = rightBuf->pageNum;
	}
	
	root->nodePage.NumOfKey = 1;
	root->nodePage.parentPageNum = 0;

	rootNum = buf_alloc_page(table_id);
	rootBuf = buf_read_page(table_id, rootNum);
	rootBuf->frame = *root;
	
	leftBuf->frame.nodePage.parentPageNum = rootNum;
	rightBuf->frame.nodePage.parentPageNum = rootNum;

	headerBuf = buf_read_page(table_id, 0);
	headerBuf->frame.headerPage.rootPageNum = rootNum;

	headerBuf->isDirty = 1;	
	rootBuf->isDirty = 1;
	leftBuf->isDirty = 1;
	rightBuf->isDirty = 1;

	rootBuf->isPinned--;
	leftBuf->isPinned--;
	rightBuf->isPinned--;
	headerBuf->isPinned--;

	free(root);
}


int get_right_index(buffer* parentBuf, pagenum_t leftNum)
{

	int right_index = 0;
	if ( parentBuf->frame.nodePage.firstPageNum == leftNum )
	{
		return right_index;
	}

	while ( right_index < parentBuf->frame.nodePage.NumOfKey &&
		   parentBuf->frame.nodePage.pageArr[right_index].pageNumber != leftNum )
		right_index++;
	return ++right_index;
}


void insert_into_node(int table_id, buffer* nBuf, int right_index, int64_t key, buffer* right)
{

	int i;
		
	for ( i = nBuf->frame.nodePage.NumOfKey; i > right_index; i-- )
	{
		nBuf->frame.nodePage.pageArr[i].key = nBuf->frame.nodePage.pageArr[i-1].key;
		nBuf->frame.nodePage.pageArr[i].pageNumber = nBuf->frame.nodePage.pageArr[i-1].pageNumber;
	}
	nBuf->frame.nodePage.pageArr[right_index].key = key;
	nBuf->frame.nodePage.pageArr[right_index].pageNumber = right->pageNum;
	nBuf->frame.nodePage.NumOfKey++;

	nBuf->isDirty = 1;
	nBuf->isPinned--;
	right->isPinned--;
}

void insert_into_node_after_splitting(int table_id, buffer* old_nodeBuf, int right_index, int64_t key, buffer* rightBuf)
{

	int i, j, split;
	pagenum_t childNum, new_nodeNum, old_nodeNum;
	int64_t k_prime;
	page_t * new_node;
	pageNumAndKey temp_factors[BRANCH_ORDER];
	buffer* new_nodeBuf;
	buffer** childBufArr;

	for ( i = 0, j = 0; i < old_nodeBuf->frame.nodePage.NumOfKey; i++, j++ )
	{
		if ( j == right_index ) j++;
		temp_factors[j].key = old_nodeBuf->frame.nodePage.pageArr[i].key;
		temp_factors[j].pageNumber = old_nodeBuf->frame.nodePage.pageArr[i].pageNumber;
	}
	temp_factors[right_index].key = key;
	temp_factors[right_index].pageNumber = rightBuf->pageNum;
	
	split = cut(branch_order);
	new_node = make_node();
	new_nodeNum = buf_alloc_page(table_id);
	new_nodeBuf = buf_read_page(table_id, new_nodeNum);
	new_nodeBuf->frame = *new_node;
	old_nodeBuf->frame.nodePage.NumOfKey = 0;


	for ( i = 0; i < split - 1; i++ )
	{
		old_nodeBuf->frame.nodePage.pageArr[i].key = temp_factors[i].key;
		old_nodeBuf->frame.nodePage.pageArr[i].pageNumber = temp_factors[i].pageNumber;
		old_nodeBuf->frame.nodePage.NumOfKey++;
	}
	k_prime = temp_factors[i].key;
	new_nodeBuf->frame.nodePage.firstPageNum = temp_factors[i].pageNumber;
	for ( ++i, j = 0; i < branch_order; i++, j++ )
	{
		new_nodeBuf->frame.nodePage.pageArr[j].key = temp_factors[i].key;
		new_nodeBuf->frame.nodePage.pageArr[j].pageNumber = temp_factors[i].pageNumber;
		new_nodeBuf->frame.nodePage.NumOfKey++;
	}
	
	new_nodeBuf->frame.nodePage.parentPageNum = old_nodeBuf->frame.nodePage.parentPageNum;

	childBufArr = (buffer**)malloc(sizeof(buffer*) * (new_nodeBuf->frame.nodePage.NumOfKey+1));
	childNum = new_nodeBuf->frame.nodePage.firstPageNum;
	childBufArr[0] = buf_read_page(table_id, childNum);
	childBufArr[0]->frame.nodePage.parentPageNum = new_nodeNum;
	childBufArr[0]->isDirty = 1;
	childBufArr[0]->isPinned--;

	for ( i = 0; i < new_nodeBuf->frame.nodePage.NumOfKey; i++ )
	{

		childNum = new_nodeBuf->frame.nodePage.pageArr[i].pageNumber;
		childBufArr[i + 1] = buf_read_page(table_id, childNum);
		childBufArr[i + 1]->frame.nodePage.parentPageNum = new_nodeNum;
		childBufArr[i + 1]->isDirty = 1;
		childBufArr[i + 1]->isPinned--;
	}

	old_nodeNum = rightBuf->frame.nodePage.parentPageNum;
	
	free(new_node);
	free(childBufArr);
	rightBuf->isPinned--;

	insert_into_parent(table_id, old_nodeBuf,  new_nodeBuf, k_prime);
}




//delete
int  db_delete(int table_id, int64_t key)
{
	pagenum_t  key_leafNum;
	buffer* key_leafBuf;
	char ret_val[120];

	
	if ( db_find(table_id, key, ret_val) == 0 )
	{
		key_leafNum = find_leaf(table_id, key);
		key_leafBuf = buf_read_page(table_id, key_leafNum);
		delete_entry(table_id, key_leafBuf, key);
		return 0;
	}
	return -1;
}

void delete_entry(int table_id, buffer* c, int64_t key)
{
	int min_keys;
	buffer* neighborBuf;
	buffer* parentBuf;
	buffer* headerBuf;
	int neighbor_index;
	int k_prime_index;
	int64_t k_prime, neighborNum;
	int capacity;

	remove_entry_from_node(table_id, c, key);
	
	headerBuf = buf_read_page(table_id, 0);
	
	if ( c->pageNum == headerBuf->frame.headerPage.rootPageNum )
	{
		adjust_root(table_id, c);
		headerBuf->isPinned--;			
		return;
	}

	min_keys = c->frame.nodePage.isLeaf ? cut(order - 1) : cut(branch_order) -1;
	if ( c->frame.nodePage.NumOfKey > 0 )
	{
		headerBuf->isPinned--;
		c->isPinned--;
		return;
	}
		
	
	neighbor_index = get_neighbor_index(table_id, c);
	if ( neighbor_index == -2 || neighbor_index == -1) k_prime_index = 0;
	else k_prime_index = neighbor_index + 1;
	
	parentBuf = buf_read_page(table_id, c->frame.nodePage.parentPageNum);
	k_prime = parentBuf->frame.nodePage.pageArr[k_prime_index].key;
	

	if ( neighbor_index == -2 ) neighborNum = parentBuf->frame.nodePage.pageArr[0].pageNumber; 
	else if ( neighbor_index == -1 ) neighborNum = parentBuf->frame.nodePage.firstPageNum;
	else neighborNum = parentBuf->frame.nodePage.pageArr[neighbor_index].pageNumber;
	
	neighborBuf = buf_read_page(table_id, neighborNum);

	capacity = c->frame.nodePage.isLeaf ? order : branch_order - 1;
	
	parentBuf->isPinned--;
	
	if ( c->frame.nodePage.NumOfKey == 0 )
	{
		
		if ( neighborBuf->frame.nodePage.NumOfKey > min_keys  )
		{
			redistribute_nodes(table_id, c, neighborBuf, neighbor_index, k_prime_index, k_prime);
			headerBuf->isPinned--;		
		}

		else
		{
			coalesce_nodes(table_id, c, neighborBuf, neighbor_index, k_prime);
			headerBuf->isPinned--;
		}

	}
	
}



void remove_entry_from_node(int table_id, buffer* nBuf, int64_t key)
{
	int i;
	// Remove the key and shift other keys accordingly.
	i = 0;
	if ( nBuf->frame.nodePage.isLeaf == 1 )
	{
		while ( nBuf->frame.nodePage.recordArr[i].key != key )
			i++;
		for ( ++i; i < nBuf->frame.nodePage.NumOfKey; i++ )
			nBuf->frame.nodePage.recordArr[i - 1] = nBuf->frame.nodePage.recordArr[i];
	}
	else
	{
		while ( nBuf->frame.nodePage.pageArr[i].key != key )
			i++;
		for ( ++i; i < nBuf->frame.nodePage.NumOfKey; i++ )
			nBuf->frame.nodePage.pageArr[i - 1] = nBuf->frame.nodePage.pageArr[i];
	}

	nBuf->frame.nodePage.NumOfKey--;
	nBuf->isDirty = 1;
}



void adjust_root(int table_id, buffer* c)
{
	buffer* new_rootBuf;
	buffer* headerBuf;
	pagenum_t new_rootNum;

	if ( c->frame.nodePage.NumOfKey > 0 )
	{
		c->isPinned--;
		c->isDirty = 1;
		return;
	}

	if ( !c->frame.nodePage.isLeaf )
	{
		new_rootNum = c->frame.nodePage.firstPageNum;
		new_rootBuf = buf_read_page(table_id, new_rootNum);
		new_rootBuf->frame.nodePage.parentPageNum = 0;
		new_rootBuf->isPinned--;	
		new_rootBuf->isDirty = 1;		

	}

	// If it is a leaf (has no children),
	// then the whole tree is empty.

	else
	{
		new_rootNum = 0;
	}
	headerBuf = buf_read_page(table_id, 0);
	headerBuf->frame.headerPage.rootPageNum = new_rootNum;
	headerBuf->isDirty = 1;
	headerBuf->isPinned--;
	c->isPinned--;
	buf_free_page(table_id, c->pageNum);
}

void redistribute_nodes(int table_id, buffer* n, buffer* neighbor, int neighbor_index, int k_prime_index, int64_t k_prime)
{
	int i, j;
	buffer* parentBuf;
	buffer** tmpBufArr;
	pagenum_t tmpNum, parentNum;
	int min_keys = n->frame.nodePage.isLeaf ? cut(order -1) : cut(branch_order) -1;

	parentNum = n->frame.nodePage.parentPageNum;
	parentBuf = buf_read_page(table_id, parentNum);

	if ( neighbor_index != -2 )
	{
		if ( n->frame.nodePage.isLeaf )
		{
			for ( i = min_keys, j = 0; i < neighbor->frame.nodePage.NumOfKey; i++, j++)
			{
				n->frame.nodePage.recordArr[j] = neighbor->frame.nodePage.recordArr[i];
				n->frame.nodePage.NumOfKey++;
			}
			parentBuf->frame.nodePage.pageArr[k_prime_index].key = n->frame.nodePage.recordArr[0].key;
			neighbor->frame.nodePage.NumOfKey = min_keys;
		}
		else
		{
			for ( i = min_keys + 1, j = 0; i < neighbor->frame.nodePage.NumOfKey; i++, j++)
			{	
				n->frame.nodePage.pageArr[j] = neighbor->frame.nodePage.pageArr[i];
				n->frame.nodePage.NumOfKey++;
			}
			n->frame.nodePage.pageArr[j].key = k_prime;
			n->frame.nodePage.pageArr[j].pageNumber = n->frame.nodePage.firstPageNum;
			n->frame.nodePage.firstPageNum = neighbor->frame.nodePage.pageArr[min_keys].pageNumber;
			n->frame.nodePage.NumOfKey++;
			
			tmpBufArr = (buffer**)malloc(sizeof(buffer*) * (j + 1));
			tmpNum = n->frame.nodePage.firstPageNum;
			tmpBufArr[0] = buf_read_page(table_id, tmpNum);
			tmpBufArr[0]->frame.nodePage.parentPageNum = n->pageNum;
			tmpBufArr[0]->isDirty = 1;
			tmpBufArr[0]->isPinned--;
			for( i = 1; i < j + 1; i++)
			{
				tmpNum = n->frame.nodePage.pageArr[i - 1].pageNumber;
				tmpBufArr[i] = buf_read_page(table_id, tmpNum);
				tmpBufArr[i]->frame.nodePage.parentPageNum = n->pageNum;	
				tmpBufArr[i]->isDirty = 1;
				tmpBufArr[i]->isPinned--;
			}
			
			parentBuf->frame.nodePage.pageArr[k_prime_index].key = neighbor->frame.nodePage.pageArr[min_keys].key;
			neighbor->frame.nodePage.NumOfKey = min_keys;

			free(tmpBufArr);
		}
	}

	else
	{
		
		if ( n->frame.nodePage.isLeaf )
		{
			for ( i = 0; i < neighbor->frame.nodePage.NumOfKey - min_keys; i++)
			{
				n->frame.nodePage.recordArr[i] = neighbor->frame.nodePage.recordArr[i];
				n->frame.nodePage.NumOfKey++;
			}
			parentBuf->frame.nodePage.pageArr[k_prime_index].key = neighbor->frame.nodePage.recordArr[i].key;
			for( i = neighbor->frame.nodePage.NumOfKey - min_keys, j = 0; i < neighbor->frame.nodePage.NumOfKey; i++, j++)
			{
				neighbor->frame.nodePage.recordArr[j] = neighbor->frame.nodePage.recordArr[i];
			}

			neighbor->frame.nodePage.NumOfKey = min_keys;

		}
		else
		{

			n->frame.nodePage.pageArr[0].key = k_prime;
			n->frame.nodePage.pageArr[0].pageNumber = neighbor->frame.nodePage.firstPageNum;
			n->frame.nodePage.NumOfKey++;
			for ( i = 0, j = 1; i < neighbor->frame.nodePage.NumOfKey - min_keys -1; i++, j++)
			{	
				n->frame.nodePage.pageArr[j] = neighbor->frame.nodePage.pageArr[i];
				n->frame.nodePage.NumOfKey++;
			}
			
			tmpBufArr = (buffer**)malloc(sizeof(buffer*) * j);
			for( i = 0; i < j; i++)
			{
				tmpNum = n->frame.nodePage.pageArr[i].pageNumber;
				tmpBufArr[i] = buf_read_page(table_id, tmpNum);
				tmpBufArr[i]->frame.nodePage.parentPageNum = n->pageNum;	
				tmpBufArr[i]->isDirty = 1;
				tmpBufArr[i]->isPinned--;
			}
			parentBuf->frame.nodePage.pageArr[k_prime_index].key = neighbor->frame.nodePage.pageArr[neighbor->frame.nodePage.NumOfKey - min_keys -1].key;
			
			neighbor->frame.nodePage.firstPageNum = neighbor->frame.nodePage.pageArr[neighbor->frame.nodePage.NumOfKey - min_keys - 1].pageNumber;	
			for( i = neighbor->frame.nodePage.NumOfKey - min_keys, j = 0; i < neighbor->frame.nodePage.NumOfKey; i++, j++)
			{
				neighbor->frame.nodePage.pageArr[j] = neighbor->frame.nodePage.pageArr[i];
			}
	
			neighbor->frame.nodePage.NumOfKey = min_keys;
			free(tmpBufArr);
		}
	}
	
	parentBuf->isDirty = 1;
	n->isDirty = 1;
	neighbor->isDirty = 1;

	parentBuf->isPinned--;
	n->isPinned--;
	neighbor->isPinned--;
}


void coalesce_nodes(int table_id, buffer* n, buffer* neighbor, int neighbor_index, int64_t k_prime)
{ 	

	buffer* parentBuf;
	buffer** childBufArr;
	pagenum_t parentNum, childNum;
	int i, j;
	parentNum = n->frame.nodePage.parentPageNum;
	if ( neighbor_index == -2 )
	{
			
		if( !n->frame.nodePage.isLeaf )
		{
			n->frame.nodePage.pageArr[0].key = k_prime;
			n->frame.nodePage.pageArr[0].pageNumber = neighbor->frame.nodePage.firstPageNum;
			
			for(i = 0, j = 1; i < neighbor->frame.nodePage.NumOfKey; i++, j++)
			{
				n->frame.nodePage.pageArr[j] = neighbor->frame.nodePage.pageArr[i];
			}
			
			childNum = neighbor->frame.nodePage.firstPageNum;
			childBufArr = (buffer**)malloc(sizeof(buffer*) * neighbor->frame.nodePage.NumOfKey +1);
			childBufArr[0] = buf_read_page(table_id, childNum);
			childBufArr[0]->frame.nodePage.parentPageNum = n->pageNum;
			childBufArr[0]->isDirty = 1;
			childBufArr[0]->isPinned--;

			for(i = 1; i < neighbor->frame.nodePage.NumOfKey + 1; i++)
			{
				childNum = neighbor->frame.nodePage.pageArr[i -1].pageNumber;
				childBufArr[i] = buf_read_page(table_id, childNum);
				childBufArr[i]->frame.nodePage.parentPageNum = n->pageNum;
				childBufArr[i]->isPinned--;
				childBufArr[i]->isDirty = 1;
			}
			free(childBufArr);

			n->frame.nodePage.NumOfKey = neighbor->frame.nodePage.NumOfKey +1;
			neighbor->frame.nodePage.NumOfKey = 0;

		}
		else
		{
			for(i = 0; i < neighbor->frame.nodePage.NumOfKey; i++)
			{
				n->frame.nodePage.recordArr[i] = neighbor->frame.nodePage.recordArr[i];
			}
			n->frame.nodePage.NumOfKey = neighbor->frame.nodePage.NumOfKey;
			neighbor->frame.nodePage.NumOfKey = 0;
			n->frame.nodePage.rightPageNum = neighbor->frame.nodePage.rightPageNum;
		}
		buf_free_page(table_id, neighbor->pageNum);
	
	}

	else
	{
		if ( !n->frame.nodePage.isLeaf )
		{

			neighbor->frame.nodePage.pageArr[neighbor->frame.nodePage.NumOfKey].key = k_prime;
			neighbor->frame.nodePage.pageArr[neighbor->frame.nodePage.NumOfKey].pageNumber = n->frame.nodePage.firstPageNum;

			childNum = n->frame.nodePage.firstPageNum;
			childBufArr = (buffer**)malloc(sizeof(buffer*));
			childBufArr[0] = buf_read_page(table_id, childNum);
					
			childBufArr[0]->frame.nodePage.parentPageNum = neighbor->pageNum;
			childBufArr[0]->isDirty = 1;
			childBufArr[0]->isPinned--;
	
			free(childBufArr);
			
			neighbor->frame.nodePage.NumOfKey++;
			
			
		}
		else neighbor->frame.nodePage.rightPageNum = n->frame.nodePage.rightPageNum;
		buf_free_page(table_id, n->pageNum);
		
	}
	
	parentBuf = buf_read_page(table_id, parentNum);
	n->isDirty = 1;
	neighbor->isDirty = 1;

	n->isPinned--;
	neighbor->isPinned--;
	
	delete_entry(table_id, parentBuf, k_prime);
	
}


int get_neighbor_index(int table_id, buffer* n)
{

	int i;
	buffer* parentBuf;
	
	parentBuf = buf_read_page(table_id, n->frame.nodePage.parentPageNum);
	parentBuf->isPinned--;
	if ( parentBuf->frame.nodePage.firstPageNum == n->pageNum ) return -2;
	for ( i = 0; i <= parentBuf->frame.nodePage.NumOfKey; i++ )
		if ( parentBuf->frame.nodePage.pageArr[i].pageNumber == n->pageNum )
			return i -1;

	// Error state.
	printf("Search for nonexistent pointer to node in parent.\n");
	exit(EXIT_FAILURE);
}


	






//프린트
void print_buf(int table_id)
{
	int i, j;
	for ( i = 0; i < BUFMAX; i++ )
	{
		if ( bufArr[i].table_id == 0 )
		{
			printf("no buffer[%d]\n", i);
			continue;
		}

		if ( bufArr[i].pageNum == 0 && bufArr[i].table_id == table_id )
		{

			printf("\nPageNum : 0\n");
			printf("freePageNum : %ld\n", bufArr[i].frame.headerPage.freePageNum);
			printf("rootPageNum : %ld\n", bufArr[i].frame.headerPage.rootPageNum);
			printf("numOfPage : %ld\n", bufArr[i].frame.headerPage.numOfPage);
			printf("isPinned : %d\n", bufArr[i].isPinned);
			printf("isDirty : %d\n", bufArr[i].isDirty);
			printf("table_id : %d\n", bufArr[i].table_id);
			printf("\n\n");
		}
		else if (bufArr[i].table_id == table_id)
		{
			printf("pageNum : %ld\n", bufArr[i].pageNum);
			printf("parentPageNum : %ld\n", bufArr[i].frame.nodePage.parentPageNum);
			printf("isPinned : %d\n", bufArr[i].isPinned);
			printf("isDirty : %d\n", bufArr[i].isDirty);
			printf("isLeaf : %d\n", bufArr[i].frame.nodePage.isLeaf);
			printf("table_id : %d\n", bufArr[i].table_id);
			if ( bufArr[i].frame.nodePage.isLeaf == 1 )
			{
				printf("rightPageNum : %ld\n", bufArr[i].frame.nodePage.rightPageNum);
				printf("key : ");
				for ( int j = 0; j < bufArr[i].frame.nodePage.NumOfKey; j++ ) printf("%ld ", bufArr[i].frame.nodePage.recordArr[j].key);
			}

			else
			{
				printf("firstPageNum : %ld\n", bufArr[i].frame.nodePage.firstPageNum);
				printf("key : ");
				for ( int j = 0; j < bufArr[i].frame.nodePage.NumOfKey; j++ ) printf("%ld ", bufArr[i].frame.nodePage.pageArr[j].key);
			}
		
			printf("\n\n");

		}
	}
}
void print_page(int table_id, pagenum_t pageNum)
{
	
	page_t* headerPage = (page_t*)malloc(sizeof(page_t));
	int index = table_id - 1;

	file_read_page(fdArr[index], 0, headerPage);

	if(pageNum >= headerPage->headerPage.numOfPage){
		free(headerPage);
		return;
	}
	page_t* page = (page_t*)malloc(sizeof(page_t));	
	
	file_read_page(fdArr[index], pageNum, page);
	
	if(pageNum == 0)
	{
		
		printf("\nPageNum : 0\n");
		printf("freePageNum : %ld\n", page->headerPage.freePageNum);
		printf("rootPageNum : %ld\n", page->headerPage.rootPageNum);
		printf("numOfPage : %ld\n", page->headerPage.numOfPage);
	}
	else
	{
		printf("pageNum : %ld\n", pageNum);
		printf("parentPageNum : %ld\n", page->nodePage.parentPageNum);
	
		if(page->nodePage.isLeaf == 1)
		{
			printf("rightPageNum : %ld\n", page->nodePage.rightPageNum);
			printf("key : ");
			for(int i = 0; i < page->nodePage.NumOfKey; i++) printf("%ld ", page->nodePage.recordArr[i].key);
		}

		else
		{
			printf("firstPageNum : %ld\n", page->nodePage.firstPageNum);
			printf("key : ");
			for(int i = 0; i < page->nodePage.NumOfKey; i++) printf("%ld ", page->nodePage.pageArr[i].key);
		}
	}
	printf("\n\n");
	free(page);
}

void print_LRU()
{
	printf("print LRU\n");
	buffer* currBuf = LRUheader.next;
	if(currBuf != NULL)
	{
		printf("LRU last : %ld\n", LRUheader.last->pageNum);
		printf("LRU next : %ld\n", LRUheader.next->pageNum);
	}
	printf("%d\n", currBuf->table_id);
	while(currBuf != NULL)	
	{
		printf("%ld, %d ->", currBuf->pageNum, currBuf->table_id);
		currBuf = currBuf->next;
	}
	printf("\n");
	
}
	
