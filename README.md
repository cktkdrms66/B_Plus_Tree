# 1. Possible call path of the insert/delete operation 

## insert() call path
(1)루트노드가 없을 때
>>insert() -> start_new_tree()

(2)해당 리프가 꽉 차있지 않을 때
>>insert() -> insert_into_leaf() 

(3)해당 리프가 꽉 차있을 때 (해당 노드가 루트노드일때)
>>insert() -> insert_into_leaf_after_splitting() -> insert_into_parent() -> insert_into_new_root()

(4)해당 리프가 꽉 차있을 때 (해당 노드가 루트노드가 아닐때) (parent가 꽉 차있지 않을때)
>>insert() -> insert_into_leaf_after_splitting() -> insert_into_parent() -> insert_into_node()

(5)해당 리프가 꽉 차있을 때 (해당 노드가 루트노드가 아닐때) (parent가 꽉 차있을 때) (parent가 루트노드일때)
>>insert() -> insert_into_leaf_after_splitting() -> insert_into_parent() -> insert_into_node_after_splitting() -> insert_into_parent() -> insert_into_new_root()

(6)해당 리프가 꽉 차있을 때 (해당 노드가 루트노드가 아닐때) (parent가 꽉 차있을 때) (parent가 루트노드가 아니고 parent의 parent가 꽉 차있지 않을 때)  
>>insert() -> insert_into_leaf_after_splitting() -> insert_into_parent() -> insert_into_node_after_splitting() -> insert_into_parent() -> insert_into_node()

(7)해당 리프가 꽉 차있을 때 (해당 노드가 루트노드가 아닐때) (parent가 꽉 차있을 때) (parent가 루트노드가 아니고 parent의 parent가 꽉 차있을 때)
>>insert() -> insert_into_leaf_after_splitting() -> insert_into_parent() -> insert_into_node_after_splitting() -> insert_into_parent() -> insert_into_node_after_splitting() -> insert_into_parent() -> ...이후 parent가 꽉 차있지 않은 경우일 땐 insert_into_node()로 끝이나고, 이후 parent가 루트노드면 insert_into_new_root()로 끝이 나고, 이후 parent가 꽉  차있으면 또 split 과정을 반복한다.






## delete() call path
(1)삭제할 key가 들어있는 노드가 루트노드이고, 리프노드일때
>>delete() -> delete_entry() -> remove_entry_from_node() -> adjust_root() 

(2)삭제할 key가 들어있는 노드가 루트노드가 아니고, key를 최소개수 이상 가지고 있을 때
>>delete() -> delete_entry() -> remove_entry_from_node()

(3)삭제할 key가 들어있는 노드가 루트노드가 아니고, key를 최소개수 이상이 아닐 때
>>delete() -> delete_entry() -> remove_entry_from_node() -> 재분배 or 병합 이 일어난다. 

(3-1)재분배(재분배하더라도 비플트리 조건 만족시키는 경우)
>>delete() -> delete_entry() -> remove_entry_from_node() -> redistribute_nodes()

(3-2)병합(재분배하면 비플트리 조건 깨지는 경우)
>>delete() -> delete_entry() -> remove_entry_from_node() -> coalesce_nodes() -> delete_entry() -> remove_entry_from_node() -> 이때에도 삭제했을 때 key를 최소개수 이상 가지고 있다면 작업이 끝이 나고, key를 최소개수 미만일 때에는 재분배 or 병합이 일어난다. 루트 노드라면 adjust_root()를 호출한다.






# 2. Detail flow of the structure modification (split, merge)

## insert() (split)
>>key 값을 트리에 삽입하는 함수이다. 경우는 크게 2가지로 나뉜다. find_leaf() 함수로 key값을 넣을 적절한 리프노드를 찾는다. key값을 넣을 리프노드가 가득차지 않았을 경우(1)와 가득찬 경우(2)로 나뉜다.

(1) 해당 리프노드가 꽉 차있지 않은 경우
>>insert_into_leaf() 함수로 key값을 넣을 리프노드를 가공한다. 넣을 자리 인덱스를 찾고, 리프노드의 키값들을 정렬한 후, 해당 인덱스에 key를 삽입한다. 포인터 배열중 해당 인덱스에 레코드를 삽입한다. 

(2) 해당 리프노드가 꽉 차있는 경우
>>insert() -> insert_into_leaf_after_splitting() -> insert_into_parent() -> insert_into_node() or insert_into_node_after_splittiong() 
꽉 차있을 경우 insert_into_leaf_after_splitting() 함수로 삽입과 스플릿을 수행한다. 새로운 리프노드를 만들고, 임시 역할을 해줄 temp_keys 와 temp_pointers 에 기존 리프노드에 있는 key값들과 삽입할 key값을 정렬하여 넣는다. 기존 리프노드를 초기화한 후, cut()함수를 이용해 구한 중간 인덱스(split) 전까지의 key값들을 기존 리프노드에 넣고, 그 뒤 key값들을 새로운 리프노드에 넣는다. 기존 리프노드가 가리키고 있는 리프노드를 새로운 리프노드가 가리키게 만들고, 기존 리프노드는 새로운 리프노드를 가리키게 만든다. 새로운 리프노드의 부모는 기존 리프노드의 부모랑 동일하다. insert_into_parent() 함수를 통해 새로운 리프노드의 0번째 key를 새로운 리프노드의 parent에 get_left_index()를 통해 구한 적절한 인덱스에 삽입을 시도한다. 이때도 새로운 리프노드의 parent가 꽉 차있지 않은 경우(2-1)와 꽉 차있는 경우(2-2)로 나뉜다.

(2-1)
>>insert_into_node() 를 통해 해당 노드를 정렬한 후, 넣을 자리에 새로운 리프노드의 0번째 key값을 삽입한다. pointers 중 (key값을 넣은 인덱스 + 1)번째 자리가 새로운 리프노드를 가리키게 한다.   

(2-2)
>>insert_into_node_after_splitting()를 통해 임시로 사용할 temp_pointers 와 temp_keys 에 parent의 pointers와 keys들을 새로운 리프노드의 0번째 key와 함께 삽입한다. 적절한 인덱스에 새로운 리프노드를 가리키게 한다. 기존 parent 노드를 초기화한 후, temp에 저장된  key들과 pointers들을 cut()를 통해 구한 split번째 인덱스전까지 parent 노드에 할당한다. split번째 후의 key들과 pointers들을 새로운 노드에 할당한다. 새로운 노드의 parent는 parent노드의 parent와 동일하다. 새로운 노드의 pointers 들은 아직 기존 노드를 가리키고 있다. 따라서 이 노드들이 새로운 노드를 가리키도록 한다. 이후, insert_into_parent()함수를 통해 또 parent노드의 parent가 꽉 차있는 경우와 꽉 차있지 않은 경우로 나뉜다. 꽉 차있지 않은 경우 그 노드를 정렬 후, k_prime(split-1번째 key)를 삽입하고, 새로운 노드를 가리키게 한다. 꽉 차있는 경우 다시 split 과정을 거친다. 계속 거치다가 루트 노드까지 도달했다면, 새로운 루트노드를 만들어 split된 노드들을 가리키게 한다.


## delete() (merge)
>>지울 key를 가진 리프노드를 find_leaf()를 통해 찾는다. delete_entry()를 통해 해당 노드의 삭제할 key를 remove_entry_from_node()를 통해 삭제한다. 키를 지우고 정렬하고, 삭제할 레코드를 삭제하고, 레코드들도 정렬한다. 이때 해당 노드가 루트 노드라면 adjust_root()를 호출한다. 이때 해당 노드가 아직 key들이 존재한다면 그대로 delete() 동작은 끝이 나고, key들이 존재하지 않는다면 트리를 동적 할당 해제를 해준다. 만약 이때 루트노드가 리프노드가 아니라면 pointers[0]이 새로운 루트 노드가 된다. 해당 노드가 루트노드가 아닌 경우, 해당 노드가 최소 가지고 있어야할 key 개수(cut()을 통해 구한 개수. 대략 절반)보다 많은 경우 delete() 동작은 끝이 난다. 그보다 적다면 인접한 노드(neighbor 노드)를 참고하여 재분배가 일어나더라도 비플트리의 조건(최소 키 개수 이상 가지고 있어야함)을 충족시킨다면 redistribute_nodes()를 호출한다. 조건을 충족시키지 못한다면 coalesce_nodes()를 호출하여 neighbor 노드와 현재 노드의 병합이 일어난다. 

재분배
>>(1)redistribute_nodes()를 호출했을 경우 재분배가 일어난다. neighbor 노드의 key들 중 가장 뒤의 있는 key를 현재 노드의 key들 중 0번째에 할당하고, pointers 또한 가장 뒤의 있는 pointers를 현재 노드의 0번째에 할당한다. 부모 노드의 k_prime_index(neighbor노드와 현재 노드 사이를 구분해주는 parent의 key index)번째 key에 바뀐 현재 노드 key[0]을 할당한다. (만약 해당 노드가 가장 left라면 neighbor 노드는 해당 노드 오른쪽 노드가 된다. 이때에는 neighbor 노드 0번째 key가 해당 노드 끝에 추가되고, 부모 노드의 k_prime_index번째 key는 neighbor 노드 key[0]이 된다.) 
(2)만약 해당 노드가 리프 노드가 아닐 경우에는 해당 노드의 key값들을 정렬한 후, 0번째에  k_prime을 삽입한다. neighbor 노드의 마지막 pointers는 해당 노드의 0번째 pointers가 가리키게 만들고, 해당 노드의 parent의 k_prime_index번째 key값은 neighbor 노드의 마지막 key값으로 바뀐다. 

병합(merge)
>>coalesce_nodes()를 호출했을 경우 병합이 일어난다. 기존 neighbor 노드에 현재 노드(key가 삭제된 노드)의 key값들을 neighbor 노드의 num_keys번째부터 채워놓는다. neighbor 노드는 기존 노드가 가리키고 있는 노드를 가리키게 한다. 기존 노드를 할당 해제해준다. delete_entry(parent node)를 통해 기존 노드의 parent를 가공한다. parent의 k_prime(병합되기 전 neighbor 노드와 노드 사이를 구분해주는 parent의 key값)를 remove_entry_from_node()를 통해 삭제하고, 다시 이 노드(parent)가 루트 노드인지, 아닌지 확인한다. 루트 노드라면 adjust_root()를 호출하고, 아니라면 k_prime이 삭제됐을 때, 최소 개수 이상인지, 아닌지 확인한다. 최소 개수 이상이면 비플트리 조건을 만족하기에 delete작업은 끝이 나고, 최소 개수 미만이면 재분배를 할지 병합을 할지 다시 인접 노드를 비교하여 정한다. 병합을 하게 된다면 앞의 방법대로 함수들을 recursive하게 호출하게 된다. 재분배를 하게 된다면 redistribute_nodes()를 호출하게 되고, 이때에는 해당 노드가 리프 노드가 아니기 때문에 (2)의 작업을 수행한다. 

