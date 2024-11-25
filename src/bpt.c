#include "bpt.h"

H_P * hp; // header page is declared as global

page * rt = NULL; //root is declared as global

int fd = -1; //file descriptor is declared as global

#define VERBOSE true

// 시작 insert 함수
int insert_into_new_root(page* left, off_t left_offset, int64_t key, page* right, off_t right_offset);
void insert_into_parent(page* left, off_t left_offset, int64_t key, page* right, off_t right_offset);
int insert_into_leaf(page* leaf, off_t offset, int64_t key, char* value);
int insert_into_leaf_or_rotate(page* leaf, off_t offset, int64_t key, char* value);
int db_insert(int64_t key, char* value);

int find_key_index(page* p, int64_t key);
int get_neighbor_index(page* , off_t p_offset);
void coalesce_nodes(page* p, off_t p_offset, page* neighbor, off_t neighbor_offset, int neighbor_index);
void redistribute_nodes(page* p, off_t p_offset, page* neighbor, off_t neighbor_offset, int neighbor_index);
int delete_entry(page* p, off_t p_offset, int key);
int db_delete(int64_t key);

void print_page(page* p, char* add_tab);
void pt();


H_P * load_header(off_t off) {
    H_P * newhp = (H_P*)calloc(1, sizeof(H_P));
    if (sizeof(H_P) > pread(fd, newhp, sizeof(H_P), 0)) {

        return NULL;
    }
    return newhp;
}

page * load_page(off_t off) {
    page* load = (page*)calloc(1, sizeof(page));
    //if (off % sizeof(page) != 0) printf("load fail : page offset error\n");
    if (sizeof(page) > pread(fd, load, sizeof(page), off)) {

        return NULL;
    }
    return load;
}

int open_table(char * pathname) {
    fd = open(pathname, O_RDWR | O_CREAT | O_EXCL | O_SYNC  , 0775);
    hp = (H_P *)calloc(1, sizeof(H_P));
    if (fd > 0) {
        //printf("New File created\n");
        hp->fpo = 0;
        hp->num_of_pages = 1;
        hp->rpo = 0;
        pwrite(fd, hp, sizeof(H_P), 0);
        free(hp);
        hp = load_header(0);
        return 0;
    }
    fd = open(pathname, O_RDWR|O_SYNC);
    if (fd > 0) {
        //printf("Read Existed File\n");
        if (sizeof(H_P) > pread(fd, hp, sizeof(H_P), 0)) {
            return -1;
        }
        off_t r_o = hp->rpo;
        rt = load_page(r_o);
        return 0;
    }
    else return -1;
}

void reset(off_t off) {
    page * reset;
    reset = (page*)calloc(1, sizeof(page));
    reset->parent_page_offset = 0;
    reset->is_leaf = 0;
    reset->num_of_keys = 0;
    reset->next_offset = 0;
    pwrite(fd, reset, sizeof(page), off);
    free(reset);
    return;
}

void freetouse(off_t fpo) {
    page * reset;
    reset = load_page(fpo);
    reset->parent_page_offset = 0;
    reset->is_leaf = 0;
    reset->num_of_keys = 0;
    reset->next_offset = 0;
    pwrite(fd, reset, sizeof(page), fpo);
    free(reset);
    return;
}

void usetofree(off_t wbf) {
    page * utf = load_page(wbf);
    utf->parent_page_offset = hp->fpo;
    utf->is_leaf = 0;
    utf->num_of_keys = 0;
    utf->next_offset = 0;
    pwrite(fd, utf, sizeof(page), wbf);
    free(utf);
    hp->fpo = wbf;
    pwrite(fd, hp, sizeof(hp), 0);
    free(hp);
    hp = load_header(0);
    return;
}

off_t new_page() {
    off_t newp;
    page * np;
    off_t prev;
    if (hp->fpo != 0) {
        newp = hp->fpo;
        np = load_page(newp);
        hp->fpo = np->parent_page_offset;
        pwrite(fd, hp, sizeof(hp), 0);
        free(hp);
        hp = load_header(0);
        free(np);
        freetouse(newp);
        return newp;
    }
    //change previous offset to 0 is needed
    newp = lseek(fd, 0, SEEK_END);
    //if (newp % sizeof(page) != 0) printf("new page made error : file size error\n");
    reset(newp);
    hp->num_of_pages++;
    pwrite(fd, hp, sizeof(H_P), 0);
    free(hp);
    hp = load_header(0);
    return newp;
}

int cut(int length) {
    if (length % 2 == 0)
        return length / 2;
    else
        return length / 2 + 1;
}

void start_new_file(record rec) {

    page * root;
    off_t ro;
    ro = new_page();
    rt = load_page(ro);
    hp->rpo = ro;
    pwrite(fd, hp, sizeof(H_P), 0);
    free(hp);
    hp = load_header(0);
    rt->num_of_keys = 1;
    rt->is_leaf = 1;
    rt->records[0] = rec;
    pwrite(fd, rt, sizeof(page), hp->rpo);
    free(rt);
    rt = load_page(hp->rpo);
}

// 탐색 함수
char* db_find(int64_t key) 
{
    if (rt == NULL) 
        return NULL;

    page* leaf = rt;
    off_t offset_child = hp->rpo;
    while (!leaf->is_leaf) 
    {
        int i;
        for (i = 0; i < leaf->num_of_keys; i++) 
            if (key < leaf->b_f[i].key)
                break;
        // 첫번째부터 탈락하면 (첫 노드보다 새 입력이 적으면) 최하위 child로 보낸다
        if (i==0) offset_child = leaf->next_offset;
        else offset_child = leaf->b_f[i-1].p_offset;
        leaf = load_page(offset_child);
    }

    int i;
    for (i = 0; i < leaf->num_of_keys; i++) 
        if (leaf->records[i].key == key) {
            return leaf->records[i].value;
        }
        
    return NULL;
}

// 이 함수는 반드시 빈 공간이 있을 때 호출되어야 함!!
int insert_into_leaf(page* leaf, off_t offset, int64_t key, char* value) {
    int i, insertion_point;

    for (insertion_point = 0; insertion_point < leaf->num_of_keys && leaf->records[insertion_point].key < key; insertion_point++);

    for (i = leaf->num_of_keys; i > insertion_point; i--)
        leaf->records[i] = leaf->records[i - 1];
    
    leaf->records[insertion_point].key = key;
    strncpy(leaf->records[insertion_point].value, value, 120);
    leaf->num_of_keys++;

    pwrite(fd, leaf, sizeof(page), offset);

    return 0;
}

// leaf node가 아닌 곳에 insert시 호출
void insert_into_parent(page* left, off_t left_offset, int64_t key, page* right, off_t right_offset) {

    // if splitting happens in the root
    if (left->parent_page_offset == 0) {
        insert_into_new_root(left, left_offset, key, right, right_offset);
        return;
    }

    page* parent = load_page(left->parent_page_offset);
    int left_index = 0;

    // 왼쪽 index를 찾는다.
    while (left_index <= parent->num_of_keys && 
           parent->b_f[left_index].p_offset != left_offset) {
        left_index++;
    }

    if (left_index == parent->num_of_keys + 1) left_index = -1;

    // Simple case: the new key fits into the node
    if (parent->num_of_keys < INTERNAL_MAX) {
        int i;
        for (i = parent->num_of_keys; i > left_index+1; i--) {
            parent->b_f[i] = parent->b_f[i-1];
        }
        parent->b_f[left_index + 1].key = key;
        parent->b_f[left_index + 1].p_offset = right_offset;
        parent->num_of_keys++;
        pwrite(fd, parent, sizeof(page), left->parent_page_offset);
        pwrite(fd, left, sizeof(page), left_offset);
        pwrite(fd, right, sizeof(page), right_offset);
        free(parent);
        return;
    }

    // 여기부터는 parent에도 빈 공간이 없는 case -> 재귀적으로 빈 공간이 생길 때까지 탐색한다.
    page* new_parent = (page*)calloc(1, sizeof(page));
    int64_t* temp_keys = (int64_t*)calloc(INTERNAL_MAX + 1, sizeof(int64_t));
    off_t* temp_offsets = (off_t*)calloc(INTERNAL_MAX + 1, sizeof(off_t));
    
    for (int i = 0, j = 0; i < parent->num_of_keys; i++, j++) {
        if (j == left_index + 1) j++;
        temp_keys[j] = parent->b_f[i].key;
        temp_offsets[j] = parent->b_f[i].p_offset;
    }
    temp_keys[left_index + 1] = key;
    temp_offsets[left_index + 1] = right_offset;
    
    new_parent->is_leaf = 0;
    new_parent->parent_page_offset = parent->parent_page_offset;
    new_parent->num_of_keys = 0;
    parent->num_of_keys = 0;
    
    int split = cut(INTERNAL_MAX);
    
    for (int i = 0; i < split-1; i++) {
        parent->b_f[i].key = temp_keys[i];
        parent->b_f[i].p_offset = temp_offsets[i];
        parent->num_of_keys++;
    }

    int k_prime = temp_keys[split - 1];
    
    for (int i = split, j = 0; i < INTERNAL_MAX+1; i++, j++) {
        new_parent->b_f[j].key = temp_keys[i];
        new_parent->b_f[j].p_offset = temp_offsets[i];
        new_parent->num_of_keys++;
    }

    free(temp_keys);
    free(temp_offsets);

    new_parent->next_offset = parent->b_f[split-1].p_offset;

    off_t new_parent_offset = new_page();

    pwrite(fd, parent, sizeof(page), left->parent_page_offset);
    pwrite(fd, new_parent, sizeof(page), new_parent_offset);
    pwrite(fd, left, sizeof(page), left_offset);
    pwrite(fd, right, sizeof(page), right_offset);

    // new_parent의 child로 들어갈 노드들의 parent를 전부 변경해 준다.
    page* child = load_page(new_parent->next_offset);
    child->parent_page_offset = new_parent_offset;
    pwrite(fd, child, sizeof(page), new_parent->next_offset);
    free(child);

    for (int i = 0; i < new_parent->num_of_keys; i++) {
        child = load_page(new_parent->b_f[i].p_offset);
        child->parent_page_offset = new_parent_offset;
        pwrite(fd, child, sizeof(page), new_parent->b_f[i].p_offset);
        free(child);
    }

    // 현재 수정하는 노드가 root인지 아닌지에 따라 호출 결정
    if (parent->parent_page_offset != 0) {
        insert_into_parent(parent, left->parent_page_offset, k_prime, new_parent, new_parent_offset);
    } else {
        insert_into_new_root(parent, left->parent_page_offset, k_prime, new_parent, new_parent_offset);
    }
    
    free(new_parent);
    free(parent);
}

// 새로운 root node를 생성해야 하는 경우 호출
int insert_into_new_root(page* left, off_t left_offset, int64_t key, page* right, off_t right_offset) {
    page* root = (page*)calloc(1, sizeof(page));
    off_t root_offset = new_page();

    root->parent_page_offset = 0;  // New root has no parent
    root->is_leaf = 0;
    root->num_of_keys = 1;
    root->b_f[0].key = key;
    root->b_f[0].p_offset = right_offset;
    root->next_offset = left_offset;

    // Update left and right children to point to new root
    left->parent_page_offset = root_offset;
    right->parent_page_offset = root_offset;

    // Write pages to disk
    pwrite(fd, root, sizeof(page), root_offset);
    pwrite(fd, left, sizeof(page), left_offset);
    pwrite(fd, right, sizeof(page), right_offset);

    // Update header to point to new root
    hp->rpo = root_offset;
    pwrite(fd, hp, sizeof(H_P), 0);

    // Update global root pointer
    free(rt);
    rt = root;
}

// 가득찬 leaf node에 새로운 key를 삽입하는 경우
int insert_into_leaf_or_rotate(page* leaf, off_t offset, int64_t key, char* value) {
    
    if (leaf->next_offset != 0)
    {
        page* next_leaf = load_page(leaf->next_offset);
        // next leaf가 존재하고 빈 공간이 있다면 rotate
        if (leaf->is_leaf && next_leaf->num_of_keys < LEAF_MAX) {
            
            int64_t* temp_keys = malloc(sizeof(int64_t) * (LEAF_MAX + 1));
            char** temp_values = malloc(sizeof(char*) * (LEAF_MAX + 1));
            int insertion_index, split, i, j;

            off_t new_page_offset = new_page();

            for (insertion_index = 0; insertion_index < LEAF_MAX && leaf->records[insertion_index].key < key; insertion_index++);

            for (i = 0, j = 0; i < leaf->num_of_keys; i++, j++) {
                if (j == insertion_index) j++;
                temp_keys[j] = leaf->records[i].key;
                temp_values[j] = leaf->records[i].value;
            }

            temp_keys[insertion_index] = key;
            temp_values[insertion_index] = value;
            leaf->num_of_keys = 0;

            // temp_values의 값들을 leaf에 복사
            for (i = 0; i < LEAF_MAX + 1; i++) {
                if (i < LEAF_MAX) {
                    leaf->records[i].key = temp_keys[i];
                    strncpy(leaf->records[i].value, temp_values[i], 120);
                    leaf->num_of_keys++;
                }
            }

            // 가장 큰 값은 next_leaf에 추가
            insert_into_leaf(next_leaf, leaf->next_offset, temp_keys[LEAF_MAX], temp_values[LEAF_MAX]);

            // 부모의 index에 있는 key값을 최댓값으로 변경
            page* parent = load_page(leaf->parent_page_offset);
            int parent_index;
            for (parent_index=0; parent->b_f[parent_index].p_offset!=leaf->next_offset; parent_index++);

            parent->b_f[parent_index].key = temp_keys[LEAF_MAX];
            pwrite(fd, parent, sizeof(page), leaf->parent_page_offset);
            pwrite(fd, leaf, sizeof(page), offset);

            free(temp_keys);
            free(temp_values);
            free(next_leaf);
            return 0;
        }

        free(next_leaf);
    }
    

    page* new_leaf;
    int64_t* temp_keys = malloc(sizeof(int64_t) * (LEAF_MAX + 1));
    char** temp_values = malloc(sizeof(char*) * (LEAF_MAX + 1));
    int insertion_index, split, i, j;

    off_t new_page_offset = new_page();
    new_leaf = load_page(new_page_offset);

    for (insertion_index = 0; insertion_index < LEAF_MAX && leaf->records[insertion_index].key < key; insertion_index++);

    for (i = 0, j = 0; i < leaf->num_of_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_keys[j] = leaf->records[i].key;
        temp_values[j] = leaf->records[i].value;
    }

    temp_keys[insertion_index] = key;
    temp_values[insertion_index] = value;

    leaf->num_of_keys = 0;

    split = cut(LEAF_MAX + 1);


    for (i = 0; i < split; i++) {
        leaf->records[i].key = temp_keys[i];
        strncpy(leaf->records[i].value, temp_values[i], 120);
        leaf->num_of_keys++;
    }

    for (i = split, j = 0; i < LEAF_MAX + 1; i++, j++) {
        new_leaf->records[j].key = temp_keys[i];
        strncpy(new_leaf->records[j].value, temp_values[i], 120);
        new_leaf->num_of_keys++;
    }

    new_leaf->next_offset = leaf->next_offset;
    leaf->next_offset = new_page_offset;

    new_leaf->parent_page_offset = leaf->parent_page_offset;
    new_leaf->is_leaf = 1;

    free(temp_keys);
    free(temp_values);

    int64_t new_key = new_leaf->records[0].key;
    insert_into_parent(leaf, offset, new_key, new_leaf, new_page_offset);

    free(new_leaf);
}

// 시작 insert 함수
int db_insert(int64_t key, char* value) {
    // root node가 비어 있으면 새 root 생성
    if (rt == NULL) {
        record new_record;
        new_record.key = key;
        strncpy(new_record.value, value, 120);
        start_new_file(new_record);
        rt->parent_page_offset = 0;
        return 0;
    }

    char* dup = db_find(key);
    if (dup != NULL) {
        return -1; // Key가 이미 존재하는 경우
    }       

    page* leaf = rt;
    off_t offset_child = hp->rpo;
    while (!leaf->is_leaf) 
    {
        int i;
        for (i = 0; i < leaf->num_of_keys; i++) 
            if (key < leaf->b_f[i].key)
                break;
        // 첫번째부터 탈락하면 (첫 노드보다 새 입력이 적으면) 최하위 child로 보낸다
        if (i==0) offset_child = leaf->next_offset;
        else offset_child = leaf->b_f[i-1].p_offset;
        leaf = load_page(offset_child);
    }

    int result;
    if (leaf->num_of_keys < LEAF_MAX) {
        result = insert_into_leaf(leaf, offset_child, key, value);
    } else {
        result = insert_into_leaf_or_rotate(leaf, offset_child, key, value);
    }

    rt = load_page(hp->rpo);

    return 0;
}

int find_key_index(page* p, int64_t key) {
    int i;

    if (p->is_leaf) {
        for (i = 0; i < p->num_of_keys; i++) {
            if (p->records[i].key == key) return i;
        }
    } else {
        for (i = 0; i < p->num_of_keys; i++) {
            if (p->b_f[i].key == key) return i;
        }
    }

    return -1;
}

// delete시 이웃 노드의 b_f상의 index를 리턴
// 기본적으로 하나 왼쪽의 index를 리턴하고, 가장 왼쪽의 child가 온 경우 -1를 리턴
int get_neighbor_index(page* p, off_t p_offset) {
    page* parent = load_page(p->parent_page_offset);
    int i;
    if (parent->next_offset == p_offset) return -2;
    for (i = 0; i <= parent->num_of_keys; i++) {
        if (parent->b_f[i].p_offset == p_offset) {
            free(parent);
            return i - 1;
        }
    }
    // Error: Should never reach here
    free(parent);
    return -1;
}

// index에 해당하는 offset을 리턴
off_t get_neighbor_offset(page* p, int index)
{
    page* parent = load_page(p->parent_page_offset);
    if (index == -2) return parent->b_f[0].p_offset;
    if (index == -1) return parent->next_offset;
    else return parent->b_f[index].p_offset;
}

// offset에 대응되는 key값을 리턴
int get_parent_key(page* p, off_t offset)
{
    page* parent = load_page(p->parent_page_offset);
    for(int i=0; i<=parent->num_of_keys; i++)
        if(parent->b_f[i].p_offset == offset)
            return parent->b_f[i].key;
    return -1;
}

// offset에 대응되는 b_f 상의 index를 리턴
int get_parent_key_index(page* p, off_t page_offset)
{
    page* parent = load_page(p->parent_page_offset);
    for(int i=0; i<p->num_of_keys; i++)
        if(parent->b_f[i].p_offset == page_offset)
            return i;
    return -1;
}

// delete 시 node 를 합칠 수 있다면 합치는 기능 구현
void coalesce_nodes(page* p, off_t p_offset, page* neighbor, off_t neighbor_offset, int neighbor_index) {
    int i, j, neighbor_insertion_index;
    page* tmp;
    off_t tmp_off;

    // 가장 왼쪽에 있는 노드라 neighbor가 오른쪽에 있다면, swap
    if (neighbor_index == -2) {
        tmp = p;
        p = neighbor;
        neighbor = tmp;
        tmp_off = p_offset;
        p_offset = neighbor_offset;
        neighbor_offset = tmp_off;
    }
    neighbor_insertion_index = neighbor->num_of_keys;

    // leaf가 아닌 경우
    if (!p->is_leaf) {
        // Append k_prime
        neighbor->b_f[neighbor_insertion_index].key = get_parent_key(p, neighbor_index);
        neighbor->b_f[neighbor_insertion_index].p_offset = p->next_offset;
        neighbor->num_of_keys++;
        // Append p's children to neighbor
        for (i = 0, j = neighbor_insertion_index + 1; i < p->num_of_keys + 1; i++, j++) {
            neighbor->b_f[j] = p->b_f[i];
            neighbor->num_of_keys++;
            p->num_of_keys--;
        }
        // Update children's parent pointers
        for (i = 0; i < neighbor->num_of_keys + 1; i++) {
            tmp = load_page(neighbor->b_f[i].p_offset);
            tmp->parent_page_offset = neighbor_offset;
            pwrite(fd, tmp, sizeof(page), neighbor->b_f[i].p_offset);
            free(tmp);
        }

        pwrite(fd, neighbor, sizeof(page), neighbor_offset);
    
    // leaf인 경우
    } else {
        // Append p's keys and pointers to neighbor
        for (i = 0, j = neighbor_insertion_index; i < p->num_of_keys; i++, j++) {
            neighbor->records[j] = p->records[i];
            neighbor->num_of_keys++;
        }
        neighbor->next_offset = p->next_offset;
        pwrite(fd, neighbor, sizeof(page), neighbor_offset);
    }

    // neighbor_index가 -2면 (p가 가장 왼쪽 노드이면) neighbor_offset을 찾는다.
    int k_prime = get_parent_key_index(p,p_offset);
    page* parent = load_page(p->parent_page_offset);
    delete_entry(parent, p->parent_page_offset, k_prime);
    // usetofree(p_offset);
}

// redist 연산 수행
// p, neighbor: p는 현재 key 개수가 부족한 node, neighbor는 p의 neighbor
// 둘 다 완결성은 있다.
// 아직 parent node등의 수정이 이루어지지는 않았음
// neighbor가 p 에 노드 하나를 퍼줘야 한다.
void redistribute_nodes(page* p, off_t p_offset, page* neighbor, off_t neighbor_offset, int neighbor_index) {
    int i;
    page* parent = load_page(p->parent_page_offset);

    // p 가 가장 왼쪽에 있는 child가 아닌 경우: b_f[0] 혹은 record[0]에 넣어준다.
    if (neighbor_index != -2) {

        // p가 leaf가 아닌 경우
        if (!p->is_leaf) {
            for (i = p->num_of_keys; i > 0; i--)
                p->b_f[i] = p->b_f[i - 1];
            p->b_f[0].key = parent->b_f[neighbor_index+1].key;
            p->next_offset = neighbor->b_f[neighbor->num_of_keys - 1].p_offset;
            parent->b_f[neighbor_index+1].key = neighbor->b_f[neighbor->num_of_keys - 1].key;
        // p가 leaf인 경우
        } else {
            for (i = p->num_of_keys; i > 0; i--)
                p->records[i] = p->records[i - 1];
            p->records[0] = neighbor->records[neighbor->num_of_keys - 1];
            parent->b_f[neighbor_index+1].key = p->records[0].key;
        }

    //p가 가장 왼쪽에 있는 child인 경우: 가장 마지막에 넣어준다.
    } else {
        if (!p->is_leaf) {
            p->b_f[p->num_of_keys].key = parent->b_f[0].key;
            p->b_f[p->num_of_keys].p_offset = neighbor->next_offset;
            parent->b_f[0].key = neighbor->b_f[1].key;
            neighbor->next_offset = neighbor->b_f[0].p_offset;
            for (i = 0; i < neighbor->num_of_keys - 1; i++)
                neighbor->b_f[i] = neighbor->b_f[i + 1];
        } else {
            p->records[p->num_of_keys] = neighbor->records[0];
            parent->b_f[0].key = neighbor->records[1].key;
            for (i = 0; i < neighbor->num_of_keys - 1; i++)
                neighbor->records[i] = neighbor->records[i + 1];
        }
    }

    p->num_of_keys++;
    neighbor->num_of_keys--;
    pwrite(fd, parent, sizeof(page), p->parent_page_offset);
    pwrite(fd, p, sizeof(page), p_offset);
    pwrite(fd, neighbor, sizeof(page), neighbor_offset);
    free(parent);
}

// root의 값을 빼서 root가 비어있게 되는 경우 재조정해주는 함수
void adjust_root()
{
    // child node가 하나라도 있다면
    if (rt->num_of_keys > 0)
    {
        off_t offset = rt->next_offset;
        rt = load_page(offset);
        rt->parent_page_offset = 0;
        pwrite(fd, rt, sizeof(page), offset);
        hp->rpo = offset;
    }
    // child node가 없다면 root를 완전히 초기화한다.
    else
    {
        rt = NULL;
        hp->rpo = 0;
    }
    pwrite(fd, hp, sizeof(H_P), 0);
}

// 특정 노드에서 특정 값을 빼는 함수
int delete_entry(page* p, off_t p_offset, int key_index) {
    int i, min_keys, neighbor_index, capacity, k_prime, k_prime_index;
    page* neighbor;
    off_t neighbor_offset;

    // key를 움직여 재조정
    for (i = key_index + 1; i < p->num_of_keys; i++) {
        if (p->is_leaf) {
            p->records[i - 1] = p->records[i];
        } else {
            p->b_f[i - 1] = p->b_f[i];
        }
    }
    p->num_of_keys--;

    // root가 텅 비게 되면 root를 재조정
    if (p->parent_page_offset == 0 && p->num_of_keys == 0) {
        adjust_root();
        return 0;
    }

    // root node라면 child의 수에 관계없이 return 0
    if (p->parent_page_offset == 0) {
        pwrite(fd, p, sizeof(page), p_offset);
        return 0;
    }

    // 키 개수를 재서 합당하면 바로 return
    min_keys = p->is_leaf ? cut(LEAF_MAX) : cut(INTERNAL_MAX) - 1;
    if (p->num_of_keys >= min_keys) {
        pwrite(fd, p, sizeof(page), p_offset);
        return 0;
    }

    // 여기 아래부터는 key 개수가 부족한 경우
    // 합치기 가장 좋은 이웃 노드를 불러온다.
    neighbor_index = get_neighbor_index(p, p_offset);
    neighbor_offset = get_neighbor_offset(p, neighbor_index);
    neighbor = load_page(neighbor_offset);
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    k_prime = load_page(p->parent_page_offset)->b_f[k_prime_index].key;

    capacity = p->is_leaf ? LEAF_MAX : INTERNAL_MAX - 1;
    // 결합 혹은 redistribute 중 적합한 것을 선책한다.
    if (neighbor->num_of_keys + p->num_of_keys < capacity) {
        coalesce_nodes(p, p_offset, neighbor, neighbor_offset, neighbor_index);
    } else {
        redistribute_nodes(p, p_offset, neighbor, neighbor_offset, neighbor_index);
    }

    return 0;
}

void print_page(page* p, char* add_tab) {
    int i;
    printf("\n");
    printf("%sParent Page offset: %ld\n", add_tab, p->parent_page_offset);
    printf("%sIs leaf: %d\n", add_tab, p->is_leaf);
    printf("%sNumber of keys: %d\n", add_tab, p->num_of_keys);
    printf("%sKeys: ", add_tab);
    for (i = 0; i < p->num_of_keys; i++) {
        if (p->is_leaf)
            printf("%-2ld ", p->records[i].key);
        else
            printf("%-2ld ", p->b_f[i].key);
    }
    printf("\n");
    if (!p->is_leaf)
    {
        print_page(load_page(p->next_offset), strcat(add_tab, "    "));
        for (int i = 0; i < p->num_of_keys; i++) 
            print_page(load_page(p->b_f[i].p_offset), add_tab);
    }
}

void pt() {
    if (rt == NULL) {
        printf("Tree is empty\n");
        return;
    }

    char add_tab[40] = "";

    print_page(rt, add_tab);
}

void pr() {
    if (rt == NULL) {
        printf("Tree is empty\n");
        return;
    }
    page* p = rt;
    int i;
    printf("\n");
    printf("Parent Page offset: %ld\n",  p->parent_page_offset);
    printf("Is leaf: %d\n",  p->is_leaf);
    printf("Number of keys: %d\n",  p->num_of_keys);
    printf("Keys: ");
    for (i = 0; i < p->num_of_keys; i++) {
        if (p->is_leaf)
            printf("%-2ld ", p->records[i].key);
        else
            printf("%-2ld ", p->b_f[i].key);
    }
    printf("\n");
}

void insert_nodes(int start, int end)
{
    for (int i = start; i < end; i++)
        db_insert(i, "test");
}

int db_delete(int64_t key) {
    if (rt == NULL) {
        return -1; // Tree is empty
    }

    char* dup = db_find(key);
    if (dup == NULL) {
        return -1; // 존재하지 않는 key를 삭제
    }

    page* leaf = rt;
    off_t offset_child = hp->rpo;
    while (!leaf->is_leaf) 
    {
        int i;
        for (i = 0; i < leaf->num_of_keys; i++) 
            if (key < leaf->b_f[i].key)
                break;
        // 첫번째부터 탈락하면 (첫 노드보다 새 입력이 적으면) 최하위 child로 보낸다
        if (i==0) offset_child = leaf->next_offset;
        else offset_child = leaf->b_f[i-1].p_offset;
        leaf = load_page(offset_child);
    }

    int key_index = find_key_index(leaf, key);
    int res = delete_entry(leaf, offset_child, key_index);

    rt = load_page(hp->rpo);

    return res;
}