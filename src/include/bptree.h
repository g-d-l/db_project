// each node has up to ORDER - 1 keys
#define ORDER 510 // gives us nodes of size 4096


// node in the b+tree
typedef struct node {
	void* values[ORDER];
	int keys[ORDER - 1];
	struct node* parent;
	int is_leaf;
	int nkeys;
	struct node* right;
} node;


// Prototypes
int find_range(node* root, int min, int max, int* found_keys, void* found_values);
node* find_leaf(node* root, int key, void* value);
node* find_leaf_first(node* root, int key);
void* find(node* root, int key);
int find_break(int length);
node* make_node(void);
node* make_leaf(void);
node* insert_into_leaf(node* leaf, int key, void* value);
node* insert_into_leaf_after_splitting(node* root, node* leaf, int key, void* value);
node* insert_into_node(node* root, node* parent, int left_index, int key, node* right);
node* insert_into_node_after_splitting(node* root, node* parent, int left_index, int key, node* right);
node* insert_into_parent(node* root, node* left, int key, node* right);
node* insert_into_new_root(node* left, int key, node* right);
node* start_new_tree(int key, void* value);
node* insert(node* root, int key, int value);
node* remove_entry_from_node(node* n, int key, node* value);
node* adjust_root(node* root);
node* merge_nodes(node* root, node* n, node* neighbor, int neighbor_index, int kp);
node* redistribute_nodes(node* root, node* n, node* neighbor, int neighbor_index, int kp_index, int kp);
node* delete_entry(node* root, node* n, int key, void* value);
node* delete(node* root, int key, void* value);
void destroy_tree(node* root);
node* bulk_load(pair* data, int len, int (*compare)(const void *, const void *));
