#include "bpt.h"

H_P * hp; // header page is declared as global

page * rt = NULL; //root is declared as global

int fd = -1; //file descriptor is declared as global


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
    //printf("new file is made\n");
}


// Helper function to find the appropriate leaf page for a key
page* find_leaf(int64_t key) {
    if (rt == NULL) return NULL;
    
    page* c = rt;
    while (!c->is_leaf) {
        int i;
        for (i = 0; i < c->num_of_keys; i++) {
            if (key < c->b_f[i].key) break;
        }
        c = load_page(i == c->num_of_keys ? c->b_f[i-1].p_offset : c->b_f[i].p_offset);
    }
    return c;
}

// Helper function to insert into a leaf
int insert_into_leaf(page* leaf, int64_t key, char* value) {
    int i, insertion_point = 0;
    while (insertion_point < leaf->num_of_keys && leaf->records[insertion_point].key < key) {
        insertion_point++;
    }
    for (i = leaf->num_of_keys; i > insertion_point; i--) {
        leaf->records[i] = leaf->records[i-1];
    }
    leaf->records[insertion_point].key = key;
    strncpy(leaf->records[insertion_point].value, value, 120);
    leaf->num_of_keys++;
    return pwrite(fd, leaf, sizeof(page), leaf->parent_page_offset);
}

// Helper function to split a leaf
int split_leaf(page* leaf, int64_t key, char* value) {
    page* new_leaf = (page*)calloc(1, sizeof(page));
    int64_t temp_keys[LEAF_MAX + 1];
    char temp_values[LEAF_MAX + 1][120];
    int insertion_index, i, j;

    for (insertion_index = 0; insertion_index < LEAF_MAX; insertion_index++) {
        if (leaf->records[insertion_index].key > key) break;
    }

    for (i = 0, j = 0; i < leaf->num_of_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_keys[j] = leaf->records[i].key;
        strncpy(temp_values[j], leaf->records[i].value, 120);
    }

    temp_keys[insertion_index] = key;
    strncpy(temp_values[insertion_index], value, 120);

    leaf->num_of_keys = 0;
    for (i = 0; i < cut(LEAF_MAX + 1); i++) {
        leaf->records[i].key = temp_keys[i];
        strncpy(leaf->records[i].value, temp_values[i], 120);
        leaf->num_of_keys++;
    }

    new_leaf->is_leaf = 1;
    new_leaf->num_of_keys = LEAF_MAX + 1 - cut(LEAF_MAX + 1);
    for (i = cut(LEAF_MAX + 1), j = 0; i < LEAF_MAX + 1; i++, j++) {
        new_leaf->records[j].key = temp_keys[i];
        strncpy(new_leaf->records[j].value, temp_values[i], 120);
    }

    new_leaf->next_offset = leaf->next_offset;
    leaf->next_offset = new_page();
    pwrite(fd, new_leaf, sizeof(page), leaf->next_offset);

    // Update parent or create a new root
    return insert_into_parent(leaf, new_leaf->records[0].key, new_leaf);
}

// Helper function to insert into parent
int insert_into_parent(page* left, int64_t key, page* right) {
    page* parent = load_page(left->parent_page_offset);
    
    if (parent == NULL) {
        // Create a new root
        page* new_root = (page*)calloc(1, sizeof(page));
        new_root->is_leaf = 0;
        new_root->num_of_keys = 1;
        new_root->b_f[0].key = key;
        new_root->b_f[0].p_offset = (off_t)left;
        new_root->b_f[1].p_offset = (off_t)right;
        left->parent_page_offset = (off_t)new_root;
        right->parent_page_offset = (off_t)new_root;
        rt = new_root;
        hp->rpo = (off_t)new_root;
        pwrite(fd, hp, sizeof(H_P), 0);
        return pwrite(fd, new_root, sizeof(page), hp->rpo);
    }

    // Find the insertion point in the parent
    int insertion_point = 0;
    while (insertion_point < parent->num_of_keys && parent->b_f[insertion_point].key < key) {
        insertion_point++;
    }

    // If there's space in the parent, insert the new key
    if (parent->num_of_keys < INTERNAL_MAX) {
        for (int i = parent->num_of_keys; i > insertion_point; i--) {
            parent->b_f[i] = parent->b_f[i-1];
        }
        parent->b_f[insertion_point].key = key;
        parent->b_f[insertion_point + 1].p_offset = (off_t)right;
        parent->num_of_keys++;
        return pwrite(fd, parent, sizeof(page), left->parent_page_offset);
    }

    // If the parent is full, split it
    return split_internal(parent, insertion_point, key, right);
}

char * db_find(int64_t key) {

}

int db_insert(int64_t key, char * value) {

    if (rt == NULL) {
        start_new_file((record){key, {0}});
        strncpy(rt->records[0].value, value, 120);
        return pwrite(fd, rt, sizeof(page), hp->rpo);
    }

    page* leaf = find_leaf(key);

    // Key already exists, update the value
    for (int i = 0; i < leaf->num_of_keys; i++) {
        if (leaf->records[i].key == key) {
            strncpy(leaf->records[i].value, value, 120);
            return pwrite(fd, leaf, sizeof(page), leaf->parent_page_offset);
        }
    }

    // Leaf has space, insert the new key-value pair
    if (leaf->num_of_keys < LEAF_MAX) {
        return insert_into_leaf(leaf, key, value);
    }

    // Leaf is full, split it
    return split_leaf(leaf, key, value);
}



int db_delete(int64_t key) {
    
}//fin








