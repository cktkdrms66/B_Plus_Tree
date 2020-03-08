#include "bpt.h"
int order = DEFAULT_ORDER;
int branch_order = BRANCH_ORDER;
int fd;
page_t* headerPage;
node* queue = NULL;

//file IO function
pagenum_t file_alloc_page()
{
		
	page_t* firstFreePage = (page_t*)malloc(sizeof(page_t));
	page_t* secondFreePage = (page_t*)malloc(sizeof(page_t));
	

	pagenum_t firstFreePageNum, secondFreePageNum;
	
	//case1 : there is no freepage 
	if ( headerPage->headerPage.freePageNum == 0 )
	{	
		headerPage->headerPage.numOfPage++;
		file_write_page(0, headerPage);
		file_read_page(0,headerPage);	
	
		return headerPage->headerPage.numOfPage -1;
	}
	//case2 : freePage exists
	firstFreePageNum = headerPage->headerPage.freePageNum;
	file_read_page(firstFreePageNum, firstFreePage);

	secondFreePageNum = firstFreePage->freePage.nextFreePageNum;
	file_read_page(secondFreePageNum, secondFreePage);

	headerPage->headerPage.freePageNum = secondFreePageNum;
	
	file_write_page(0, headerPage);
	return firstFreePageNum;

}


void file_free_page(pagenum_t pagenum)
{

	page_t* firstFreePage = (page_t*)malloc(sizeof(page_t));
	page_t* currPage = (page_t*)malloc(sizeof(page_t));
	pagenum_t firstFreePageNum;
		
	firstFreePageNum = headerPage->headerPage.freePageNum;
	file_read_page(firstFreePageNum, firstFreePage);

	currPage->freePage.nextFreePageNum = firstFreePageNum;
	currPage->nodePage.NumOfKey = 0;
	
	headerPage->headerPage.freePageNum = pagenum;
	
	file_write_page(0, headerPage);
	file_write_page(pagenum, currPage);
	
	free(firstFreePage);
	free(currPage);
}

void file_read_page(pagenum_t pagenum, page_t* dest)
{
	lseek(fd, pagenum * BUFFSIZE, SEEK_SET);
	read(fd, dest, BUFFSIZE);
}

void file_write_page(pagenum_t pagenum, const page_t* src)
{
	lseek(fd, pagenum * BUFFSIZE, SEEK_SET);
	write(fd, src, BUFFSIZE);
}



int open_table(char* pathname)
{
	int table_id = 0;

	mode_t mode = S_IRUSR | S_IWUSR| S_IRGRP| S_IWGRP| S_IROTH| S_IWOTH;
		
	fd = open(pathname, O_RDWR | O_CREAT | O_EXCL| O_SYNC, mode);
	
			
	if(fd == -1)
	{
		fd = open(pathname, O_RDWR | O_SYNC);
		headerPage = (page_t*)malloc(sizeof(page_t));
		file_read_page(0, headerPage);
	}
			

	if(lseek(fd, 0, SEEK_END) == 0)
	{
		headerPage = (page_t*)malloc(sizeof(page_t));
	
		//make headerpage
		headerPage->headerPage.numOfPage = 1;
		headerPage->headerPage.rootPageNum = 0;
		headerPage->headerPage.freePageNum = 0;
			
		file_write_page(0, headerPage);
		
	}

	return table_id;
}

void close_table()
{
	free(headerPage);
	close(fd);
}

int db_find(int64_t key, char * ret_val)
{
	int i = 0;
	page_t* c = (page_t*)malloc(sizeof(page_t));
	pagenum_t cNum = find_leaf(key);
	if ( cNum == 0 ){
		free(c);
		return -1;
	}
	
	file_read_page(cNum, c);
	
	for ( i = 0; i < c->nodePage.NumOfKey; i++ )
		if ( c->nodePage.recordArr[i].key == key ) break;
	
	if ( i == c->nodePage.NumOfKey )
	{
		
		free(c);
		return -1;
	}
	else
	{
		//레코드의 value를 ret_val에 담기
		for ( int j = 0; j < 120; j++ )
		{
			ret_val[j] = c->nodePage.recordArr[i].value[j];
		}
		free(c);
		return 0;
	}
		
}

int db_insert(int64_t key, char* value)
{

	record* newRecord;
	page_t* root;
	page_t* leaf;
	pagenum_t insertNum;
	pagenum_t leafNum;
	
	char ret_val[120];

	//해당 키가 이미 존재한다면 
	if ( db_find(key, ret_val) == 0 ) return -1;
	
	newRecord = make_record(key, value);
	
	//루트페이지가 없을때
	if ( headerPage->headerPage.rootPageNum == 0 )
	{
		return start_new_tree(newRecord);
	}

	insertNum = find_leaf(key);

	leaf = (page_t*)malloc(sizeof(page_t));
	file_read_page(insertNum, leaf);	
		
	
	//리프노드에 빈자리가 있을 때
	if ( leaf->nodePage.NumOfKey < order - 1 )
	{
		insert_into_leaf(insertNum, leaf, key, newRecord);
		return 0;
	}

	//리프노드에 빈자리가 없을 때
	insert_into_leaf_after_splitting(insertNum, leaf, key, newRecord);
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


int start_new_tree(record * newRecord)
{
	page_t* root = make_leaf();
	pagenum_t rootNum = file_alloc_page();

	headerPage->headerPage.rootPageNum = 1;
	root->nodePage.recordArr[0].key = newRecord->key;
	for(int i = 0; i < 120; i++) root->nodePage.recordArr[0].value[i] = newRecord->value[i];
	
	root->nodePage.firstPageNum = 0;
	root->nodePage.parentPageNum = 0;
	root->nodePage.NumOfKey = 1;
	
	file_write_page(0, headerPage);
	file_write_page(rootNum, root);

	free(newRecord);
	free(root);
	
	return 0;
}

pagenum_t find_leaf( int64_t key)
{
	
	int i;
	page_t* c = (page_t*)malloc(sizeof(page_t));
	pagenum_t cNum = headerPage->headerPage.rootPageNum;
	
	if ( headerPage->headerPage.rootPageNum == 0 )
	{
		free(c);
		return 0;
	}
	
	file_read_page(cNum, c);

	
	while ( c->nodePage.isLeaf != 1 )
	{
		i = 0;
		while ( i < c->nodePage.NumOfKey )
		{
			if( key < c->nodePage.pageArr[0].key)
			{
				i = -1;
				break;
			}
			if( key >= c->nodePage.pageArr[i].key) i++;
			else break;
		}
		
		if ( i == -1 )
		{
			cNum = c->nodePage.firstPageNum;
			file_read_page(cNum, c);
		}
		else
		{
			cNum = c->nodePage.pageArr[i -1].pageNumber;
			file_read_page(cNum, c);
		}
		
	}



	free(c);
	
	return cNum;
}

int cut(int length)
{
	if ( length % 2 == 0 )
		return length / 2;
	else
		return length / 2 + 1;
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

void insert_into_leaf(pagenum_t insertNum, page_t * leaf, int64_t key, record * pointer)
{
	int i, insertion_point;
	
	insertion_point = 0;
	while ( insertion_point < leaf->nodePage.NumOfKey && leaf->nodePage.recordArr[insertion_point].key < key )
		insertion_point++;

	for ( i = leaf->nodePage.NumOfKey; i > insertion_point; i-- )
	{
		leaf->nodePage.recordArr[i] = leaf->nodePage.recordArr[i - 1];
	}

	leaf->nodePage.recordArr[insertion_point].key = pointer->key;
	for(int j = 0; j < 120; j++)
		leaf->nodePage.recordArr[insertion_point].value[j] = pointer->value[j];
	
	leaf->nodePage.NumOfKey++;
	
	file_write_page(insertNum, leaf);
	
	free(leaf);
	free(pointer);
}


void insert_into_leaf_after_splitting(pagenum_t insertNum, page_t * leaf, int64_t key, record * pointer)
{
	page_t* new_leaf;
	record  temp_records[DEFAULT_ORDER];
	pagenum_t new_leafNum;
	int insertion_index, split, new_key, i, j;

	new_leaf = make_leaf();
	new_leafNum = file_alloc_page();

	insertion_index = 0;
	while ( insertion_index < order - 1 && leaf->nodePage.recordArr[insertion_index].key < key )
		insertion_index++;

	for ( i = 0, j = 0; i < leaf->nodePage.NumOfKey; i++, j++ )
	{
		if ( j == insertion_index ) j++;
		temp_records[j] = leaf->nodePage.recordArr[i];
	}
	temp_records[insertion_index] = *pointer;
	
	leaf->nodePage.NumOfKey = 0;

	split = cut(order - 1);

	for ( i = 0; i < split; i++ )
	{
		leaf->nodePage.recordArr[i] = temp_records[i];
		leaf->nodePage.NumOfKey++;
	}

	for ( i = split, j = 0; i < order; i++, j++ )
	{
		new_leaf->nodePage.recordArr[j] = temp_records[i];
		new_leaf->nodePage.NumOfKey++;
	}
	
	new_leaf->nodePage.rightPageNum = leaf->nodePage.rightPageNum;
	leaf->nodePage.rightPageNum = new_leafNum;
	
	for ( i = leaf->nodePage.NumOfKey; i < order - 1; i++ )
	{
		leaf->nodePage.recordArr[i].key = 0;
		for ( int k = 0; k < 120; k++ )
		{
			leaf->nodePage.recordArr[i].value[k] = 0;
 		}
	}

	for ( i = leaf->nodePage.NumOfKey; i < order - 1; i++ )
	{
		new_leaf->nodePage.recordArr[i].key = 0;
		for ( int k = 0; k < 120; k++ )
		{
			leaf->nodePage.recordArr[i].value[k] = 0;
		}
	}
	new_leaf->nodePage.parentPageNum = leaf->nodePage.parentPageNum;
	new_key = new_leaf->nodePage.recordArr[0].key;

	file_write_page(insertNum, leaf);
	file_write_page(new_leafNum, new_leaf);
	
	insert_into_parent(leaf, new_leaf, new_key, insertNum, new_leafNum);
}


void insert_into_parent(page_t * left, page_t * right, int64_t key, pagenum_t leftNum, pagenum_t rightNum)
{

	int right_index;
	page_t* parent;
	pagenum_t parentNum;

	parentNum = left->nodePage.parentPageNum;

	/* Case: new root. */
	if ( parentNum == 0 )
	{
		insert_into_new_root(left, right, key, leftNum, rightNum);
		return;
	}
	
	free(left);
	
	parent = (page_t*)malloc(sizeof(page_t));
	file_read_page(parentNum, parent);
	right_index = get_right_index(parent, leftNum);

	if ( parent->nodePage.NumOfKey < branch_order - 1 )
	{
		insert_into_node(parent, right_index, key, right, rightNum);
		return;
	}
	
	insert_into_node_after_splitting(parent, right_index, key, right, rightNum);
}


void insert_into_new_root(page_t * left, page_t * right, int64_t k_prime, pagenum_t leftNum, pagenum_t rightNum)
{

	page_t* root = make_node();
	pagenum_t rootNum;
	if ( left->nodePage.isLeaf == 1 )
	{
		root->nodePage.firstPageNum = leftNum;
		root->nodePage.pageArr[0].key = k_prime;
		root->nodePage.pageArr[0].pageNumber = rightNum;
	}
	else
	{
		root->nodePage.firstPageNum = leftNum;
		root->nodePage.pageArr[0].key = k_prime;
		root->nodePage.pageArr[0].pageNumber = rightNum;
	}
	
	root->nodePage.NumOfKey = 1;
	root->nodePage.parentPageNum = 0;

	rootNum = file_alloc_page();

	left->nodePage.parentPageNum = rootNum;
	right->nodePage.parentPageNum = rootNum;
	
	headerPage->headerPage.rootPageNum = rootNum;
	
	file_write_page(rootNum, root);
	file_write_page(leftNum, left);
	file_write_page(rightNum, right);
	file_write_page(0, headerPage);

	free(root);
	free(left);
	free(right);
}


int get_right_index(page_t * parent, pagenum_t leftNum)
{

	int right_index = 0;
	if ( parent->nodePage.firstPageNum == leftNum )
	{
		return right_index;
	}

	while ( right_index < parent->nodePage.NumOfKey &&
		   parent->nodePage.pageArr[right_index].pageNumber != leftNum )
		right_index++;
	return ++right_index;
}


void insert_into_node(page_t * n, int right_index, int64_t key, page_t * right, pagenum_t rightNum)
{

	int i;
		
	for ( i = n->nodePage.NumOfKey; i > right_index; i-- )
	{
		n->nodePage.pageArr[i].key = n->nodePage.pageArr[i-1].key;
		n->nodePage.pageArr[i].pageNumber = n->nodePage.pageArr[i-1].pageNumber;
	}
	n->nodePage.pageArr[right_index].key = key;
	n->nodePage.pageArr[right_index].pageNumber = rightNum;
	n->nodePage.NumOfKey++;

	file_write_page(right->nodePage.parentPageNum, n);
	file_write_page(rightNum, right);
	file_write_page(0, headerPage);

	free(n);
	free(right);
}

void insert_into_node_after_splitting(page_t * old_node, int right_index, int64_t key, page_t * right, pagenum_t rightNum)
{

	int i, j, split;
	pagenum_t childNum, new_nodeNum, old_nodeNum;
	int64_t k_prime;
	page_t * new_node = (page_t*)malloc(sizeof(page_t));
	page_t * child = (page_t*)malloc(sizeof(page_t));
	pageNumAndKey temp_factors[BRANCH_ORDER];
	
	for ( i = 0, j = 0; i < old_node->nodePage.NumOfKey; i++, j++ )
	{
		if ( j == right_index ) j++;
		temp_factors[j].key = old_node->nodePage.pageArr[i].key;
		temp_factors[j].pageNumber = old_node->nodePage.pageArr[i].pageNumber;
	}
	temp_factors[right_index].key = key;
	temp_factors[right_index].pageNumber = rightNum;
	
	split = cut(branch_order);
	new_node = make_node();
	new_nodeNum = file_alloc_page();
	old_node->nodePage.NumOfKey = 0;
	for ( i = 0; i < split - 1; i++ )
	{
		old_node->nodePage.pageArr[i].key = temp_factors[i].key;
		old_node->nodePage.pageArr[i].pageNumber = temp_factors[i].pageNumber;
		old_node->nodePage.NumOfKey++;
	}
	k_prime = temp_factors[i].key;
	new_node->nodePage.firstPageNum = temp_factors[i].pageNumber;
	for ( ++i, j = 0; i < branch_order; i++, j++ )
	{
		new_node->nodePage.pageArr[j].key = temp_factors[i].key;
		new_node->nodePage.pageArr[j].pageNumber = temp_factors[i].pageNumber;
		new_node->nodePage.NumOfKey++;
	}
	
	new_node->nodePage.parentPageNum = old_node->nodePage.parentPageNum;

	childNum = new_node->nodePage.firstPageNum;
	file_read_page(childNum, child);
	child->nodePage.parentPageNum = new_nodeNum;
	file_write_page(childNum, child);
	for ( i = 0; i <= new_node->nodePage.NumOfKey; i++ )
	{
		childNum = new_node->nodePage.pageArr[i].pageNumber;
		file_read_page(childNum, child);
		child->nodePage.parentPageNum = new_nodeNum;
		file_write_page(childNum, child);
	}

	old_nodeNum = right->nodePage.parentPageNum;

	file_write_page(new_nodeNum, new_node);
	file_write_page(old_nodeNum, old_node);
	

	free(right);
	free(child);

	insert_into_parent(old_node,  new_node, k_prime, old_nodeNum ,new_nodeNum);
}




//delete
int  db_delete(int64_t key)
{
	pagenum_t  key_leafNum;
	page_t * key_leaf;
	char ret_val[120];

	
	if ( db_find(key, ret_val) == 0 )
	{
		key_leafNum = find_leaf(key);
		key_leaf = (page_t*)malloc(sizeof(page_t));
		file_read_page(key_leafNum, key_leaf);
		delete_entry(key_leafNum, key_leaf,key);
		return 0;
	}
	return -1;
}

void delete_entry(pagenum_t cNum, page_t* c,int64_t key)
{

	int min_keys;
	page_t * neighbor;
	page_t * parent;
	int neighbor_index;
	int k_prime_index;
	int64_t k_prime, neighborNum;
	int capacity;

	remove_entry_from_node(cNum, c, key);

	if ( cNum == headerPage->headerPage.rootPageNum )
	{
		adjust_root(cNum, c);
		return;
	}

	min_keys = c->nodePage.isLeaf ? cut(order - 1) : cut(branch_order) -1;

	if ( c->nodePage.NumOfKey > 0 )
	{
		return;
	}
		
	
	neighbor_index = get_neighbor_index(cNum, c);
	if ( neighbor_index == -2 || neighbor_index == -1) k_prime_index = 0;
	else k_prime_index = neighbor_index + 1;
	

	parent = (page_t*)malloc(sizeof(page_t));
	file_read_page(c->nodePage.parentPageNum, parent);

	k_prime = parent->nodePage.pageArr[k_prime_index].key;
	

	neighbor = (page_t*)malloc(sizeof(page_t));
	if ( neighbor_index == -2 ) neighborNum = parent->nodePage.pageArr[0].pageNumber; 
	else if ( neighbor_index == -1 ) neighborNum = parent->nodePage.firstPageNum;
	else neighborNum = parent->nodePage.pageArr[neighbor_index].pageNumber;
	file_read_page(neighborNum, neighbor);


	capacity = c->nodePage.isLeaf ? order : branch_order - 1;
	
	if ( c->nodePage.NumOfKey == 0 )
	{
		
		if ( neighbor->nodePage.NumOfKey > min_keys  )
		{
			redistribute_nodes(cNum, neighborNum, c, neighbor, neighbor_index, k_prime_index, k_prime);
		}

		else
		{
			coalesce_nodes(cNum, neighborNum, c, neighbor, neighbor_index, k_prime);
		}

	}
	

	free(parent);
	

}



void remove_entry_from_node(pagenum_t nNum, page_t* n, int64_t key)
{
	int i;
	

	// Remove the key and shift other keys accordingly.
	i = 0;
	if ( n->nodePage.isLeaf == 1 )
	{
		while ( n->nodePage.recordArr[i].key != key )
			i++;
		for ( ++i; i < n->nodePage.NumOfKey; i++ )
			n->nodePage.recordArr[i - 1] = n->nodePage.recordArr[i];
	}
	else
	{
		while ( n->nodePage.pageArr[i].key != key )
			i++;
		for ( ++i; i < n->nodePage.NumOfKey; i++ )
			n->nodePage.pageArr[i - 1] = n->nodePage.pageArr[i];
	}

	n->nodePage.NumOfKey--;

	file_write_page(nNum, n);

}



void adjust_root(pagenum_t cNum, page_t* c)
{

	page_t * new_root;
	pagenum_t new_rootNum;

	if ( c->nodePage.NumOfKey > 0 )
		return;

	if ( !c->nodePage.isLeaf )
	{
		new_rootNum = c->nodePage.firstPageNum;
		
		new_root = (page_t*)malloc(sizeof(page_t));
		file_read_page(new_rootNum, new_root);
		new_root->nodePage.parentPageNum = 0;
			
		file_write_page(new_rootNum, new_root);
		
		free(new_root);
	}

	// If it is a leaf (has no children),
	// then the whole tree is empty.

	else
	{
		new_rootNum = 0;
	}
	headerPage->headerPage.rootPageNum = new_rootNum;
	file_free_page(cNum);


	
	free(c);

	
}

void redistribute_nodes(pagenum_t nNum, pagenum_t neighborNum, page_t * n, page_t * neighbor, int neighbor_index,
						int k_prime_index, int64_t k_prime)
{
	int i, j;
	page_t * tmp;
	page_t * parent;
	pagenum_t tmpNum, parentNum;
	int min_keys = n->nodePage.isLeaf ? cut(order -1) : cut(branch_order) -1;

	parent = (page_t*)malloc(sizeof(page_t));
	parentNum = n->nodePage.parentPageNum;
	file_read_page(parentNum, parent);

	if ( neighbor_index != -2 )
	{
		if ( n->nodePage.isLeaf )
		{
			for ( i = min_keys, j = 0; i < neighbor->nodePage.NumOfKey; i++, j++)
			{
				n->nodePage.recordArr[j] = neighbor->nodePage.recordArr[i];
				n->nodePage.NumOfKey++;
			}
			parent->nodePage.pageArr[k_prime_index].key = n->nodePage.recordArr[0].key;
			neighbor->nodePage.NumOfKey = min_keys;
		}
		else
		{
			for ( i = min_keys + 1, j = 0; i < neighbor->nodePage.NumOfKey; i++, j++)
			{	
				n->nodePage.pageArr[j] = neighbor->nodePage.pageArr[i];
				n->nodePage.NumOfKey++;
			}
			n->nodePage.pageArr[j].key = k_prime;
			n->nodePage.pageArr[j].pageNumber = n->nodePage.firstPageNum;
			n->nodePage.firstPageNum = neighbor->nodePage.pageArr[min_keys].pageNumber;
			n->nodePage.NumOfKey++;
			
			tmpNum = n->nodePage.firstPageNum;
			tmp = (page_t*)malloc(sizeof(page_t));
			file_read_page(tmpNum, tmp);
			tmp->nodePage.parentPageNum = nNum;
			file_write_page(tmpNum, tmp);
			for( i = 0; i < j; i++)
			{
				tmpNum = n->nodePage.pageArr[i].pageNumber;
				file_read_page(tmpNum, tmp);
				tmp->nodePage.parentPageNum = nNum;	
				file_write_page(tmpNum, tmp);
			}
			
			parent->nodePage.pageArr[k_prime_index].key = neighbor->nodePage.pageArr[min_keys].key;
			neighbor->nodePage.NumOfKey = min_keys;
		}
	}

	else
	{
		
		if ( n->nodePage.isLeaf )
		{
			for ( i = 0; i < neighbor->nodePage.NumOfKey - min_keys; i++)
			{
				n->nodePage.recordArr[i] = neighbor->nodePage.recordArr[i];
				n->nodePage.NumOfKey++;
			}
			parent->nodePage.pageArr[k_prime_index].key = neighbor->nodePage.recordArr[i].key;
			for( i = neighbor->nodePage.NumOfKey - min_keys, j = 0; i < neighbor->nodePage.NumOfKey; i++, j++)
			{
				neighbor->nodePage.recordArr[j] = neighbor->nodePage.recordArr[i];
			}

			neighbor->nodePage.NumOfKey = min_keys;

		}
		else
		{

			n->nodePage.pageArr[0].key = k_prime;
			n->nodePage.pageArr[0].pageNumber = neighbor->nodePage.firstPageNum;
			n->nodePage.NumOfKey++;
			for ( i = 0, j = 1; i < neighbor->nodePage.NumOfKey - min_keys -1; i++, j++)
			{	
				n->nodePage.pageArr[j] = neighbor->nodePage.pageArr[i];
				n->nodePage.NumOfKey++;
			}
			
		        tmp = (page_t*)malloc(sizeof(page_t));
			for( i = 0; i < j; i++)
			{
				tmpNum = n->nodePage.pageArr[i].pageNumber;
				file_read_page(tmpNum, tmp);
				tmp->nodePage.parentPageNum = nNum;	
				file_write_page(tmpNum, tmp);
			}
			parent->nodePage.pageArr[k_prime_index].key = neighbor->nodePage.pageArr[neighbor->nodePage.NumOfKey - min_keys -1].key;
			
			neighbor->nodePage.firstPageNum = neighbor->nodePage.pageArr[neighbor->nodePage.NumOfKey - min_keys - 1].pageNumber;	
			for( i = neighbor->nodePage.NumOfKey - min_keys, j = 0; i < neighbor->nodePage.NumOfKey; i++, j++)
			{
				neighbor->nodePage.pageArr[j] = neighbor->nodePage.pageArr[i];
			}
	
			neighbor->nodePage.NumOfKey = min_keys;
		}
	}
	

	file_write_page(nNum, n);
	file_write_page(neighborNum, neighbor);
	file_write_page(parentNum, parent);
	if ( !n->nodePage.isLeaf ) free(tmp);

	free(parent);
	free(n);
	free(neighbor);
}


void coalesce_nodes(pagenum_t nNum, pagenum_t neighborNum, page_t * n, page_t * neighbor, int neighbor_index, int64_t k_prime)
{

	page_t * parent;
	page_t * child;
	pagenum_t parentNum, childNum;
	int i, j;

	if ( neighbor_index == -2 )
	{
			
		if( !n->nodePage.isLeaf )
		{
			n->nodePage.pageArr[0].key = k_prime;
			n->nodePage.pageArr[0].pageNumber = neighbor->nodePage.firstPageNum;
			
			for(i = 0, j = 1; i < neighbor->nodePage.NumOfKey; i++, j++)
			{
				n->nodePage.pageArr[j] = neighbor->nodePage.pageArr[i];
			}
			
			childNum = neighbor->nodePage.firstPageNum;
			child = (page_t*)malloc(sizeof(page_t));
			file_read_page(childNum, child);
			child->nodePage.parentPageNum = nNum;
			file_write_page(childNum, child);		

			for(i = 0; i < neighbor->nodePage.NumOfKey; i++)
			{
				childNum = neighbor->nodePage.pageArr[i].pageNumber;
				file_read_page(childNum, child);
				child->nodePage.parentPageNum = nNum;
				file_write_page(childNum, child);	
			}
			free(child);

			n->nodePage.NumOfKey = neighbor->nodePage.NumOfKey +1;
			neighbor->nodePage.NumOfKey = 0;
		}
		else
		{
			for(i = 0; i < neighbor->nodePage.NumOfKey; i++)
			{
				n->nodePage.recordArr[i] = neighbor->nodePage.recordArr[i];
			}
			n->nodePage.NumOfKey = neighbor->nodePage.NumOfKey;
			neighbor->nodePage.NumOfKey = 0;
			n->nodePage.rightPageNum = neighbor->nodePage.rightPageNum;
		}
		
		file_free_page(neighborNum);
		file_write_page(nNum, n);
	}

	else
	{
		if ( !n->nodePage.isLeaf )
		{

			neighbor->nodePage.pageArr[neighbor->nodePage.NumOfKey].key = k_prime;
			neighbor->nodePage.pageArr[neighbor->nodePage.NumOfKey].pageNumber = n->nodePage.firstPageNum;

			childNum = n->nodePage.firstPageNum;
			child = (page_t*)malloc(sizeof(page_t));
			file_read_page(childNum, child);
			child->nodePage.parentPageNum = neighborNum;
			file_write_page(childNum, child);		
			free(child);
			
			neighbor->nodePage.NumOfKey++;
			
			
		}
		else neighbor->nodePage.rightPageNum = n->nodePage.rightPageNum;
		file_free_page(nNum);
		file_write_page(neighborNum, neighbor);
	}
	
	
	parentNum = neighbor->nodePage.parentPageNum;
	parent = (page_t*)malloc(sizeof(page_t));
	file_read_page(parentNum, parent);
	
	
	free(n);
	free(neighbor);
	
	delete_entry(parentNum, parent, k_prime);
	
	
}


int get_neighbor_index(pagenum_t nNum, page_t* n)
{

	int i;
	page_t parent;
	file_read_page(n->nodePage.parentPageNum, &parent);

	if ( parent.nodePage.firstPageNum == nNum ) return -2;
	for ( i = 0; i <= parent.nodePage.NumOfKey; i++ )
		if ( parent.nodePage.pageArr[i].pageNumber == nNum )
			return i -1;

	// Error state.
	printf("Search for nonexistent pointer to node in parent.\n");
	printf("Node:  %#lx\n", (unsigned long)n);
	exit(EXIT_FAILURE);
}


	






//프린트
void print_page(pagenum_t pageNum)
{
	if(pageNum >= headerPage->headerPage.numOfPage) return;
	page_t* page = (page_t*)malloc(sizeof(page_t));	
	
	file_read_page(pageNum, page);
	
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

	
