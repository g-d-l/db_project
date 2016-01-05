#include <stdlib.h>
#include <stdio.h>
#include "comparators.h"
#include "bptree.h"


int find_range(node* root, int min, int max, int* found_keys, void* found_values) {
	int num_found = 0;
	node* n = find_leaf_first(root, min);
	if (n == NULL) 
		return 0;
	int i = 0;
	while (i < n->nkeys && n->keys[i] < min)
		i++;
	while (n != NULL) {
		for (; i < n->nkeys && n->keys[i] <= max; i++) {
			if (found_keys)
				found_keys[num_found] = n->keys[i];
			((int*) found_values)[num_found] = (int) n->values[i];
			num_found++;
		}
		n = (node*) n->values[ORDER - 1];
		i = 0;
	}
	return num_found;
}


// find the leaf that contains the given key
node* find_leaf(node* root, int key, void* value) {
	int i = 0;
	node* curr = root;
	if (curr == NULL) {
		return curr;
	}
	while (!curr->is_leaf) {
		i = 0;
		while (i < curr->nkeys) {
			if (key > curr->keys[i]) 
				i++;
			else 
				break;
		}
		curr = (node*) curr->values[i];
	}
	while (curr != NULL) {
		i = 0;
		while (i < curr->nkeys && !(curr->values[i] == value && curr->keys[i] == key))
			i++;
		if (i == curr->nkeys)
			curr = curr->values[ORDER - 1];
		else
			return curr;
	}
	return NULL;
}

node* find_leaf_first(node* root, int key) {
	int i = 0;
	node* curr = root;
	if (curr == NULL) {
		return curr;
	}
	while (!curr->is_leaf) {
		i = 0;
		while (i < curr->nkeys) {
			if (key > curr->keys[i]) 
				i++;
			else 
				break;
		}
		curr = (node*) curr->values[i];
	}
	return curr;
}



// find the value for a given key
void* find(node* root, int key) {
	int i = 0;
	node* curr = find_leaf_first(root, key);
	if (curr == NULL)
		return NULL;
	for (i = 0; i < curr->nkeys; i++) {
		if (curr->keys[i] == key)
			break;
	}
	if (i == curr->nkeys) 
		return NULL;
	else
		return curr->values[i];
}

// find where to split a node 
int find_break(int len) {
	if (len % 2 == 0)
		return len / 2;
	else
		return len / 2 + 1;
}



// make node, either for internal use or as a leaf
node* make_node(void) {
	node* new_node = malloc(sizeof(node));
	new_node->is_leaf = 0;
	new_node->nkeys = 0;
	new_node->parent = NULL;
	new_node->right = NULL;
	return new_node;
}

// generate a leaf
node* make_leaf(void) {
	node* leaf = make_node();
	leaf->is_leaf = 1;
	return leaf;
}

// insert into leaf and return the new lead
node* insert_into_leaf(node* leaf, int key, void* value) {
	int insertion_point = 0;
	while (insertion_point < leaf->nkeys && leaf->keys[insertion_point] < key)
		insertion_point++;
	for (int i = leaf->nkeys; i > insertion_point; i--) {
		leaf->keys[i] = leaf->keys[i - 1];
		leaf->values[i] = leaf->values[i - 1];
	}
	leaf->keys[insertion_point] = key;
	leaf->values[insertion_point] = value;
	leaf->nkeys++;
	return leaf;
}


// insert and cause a split in the leaf
node* insert_into_leaf_after_splitting(node* root, node* leaf, int key, void* value) {
	node* new_leaf = make_leaf();

	int* temp_keys = malloc(ORDER * sizeof(int));

	int** temp_pointers = malloc( ORDER * sizeof(void *) );
	int insertion_index = 0;
	while (insertion_index < ORDER - 1 && leaf->keys[insertion_index] < key)
		insertion_index++;

    int j = 0;
	for (int i = 0; i < leaf->nkeys; i++, j++) {
		if (j == insertion_index)
			j++;
		temp_keys[j] = leaf->keys[i];
		temp_pointers[j] = leaf->values[i];
	}

	temp_keys[insertion_index] = key;
	temp_pointers[insertion_index] = value;
	leaf->nkeys = 0;
	int split = find_break(ORDER - 1);

	for (int i = 0; i < split; i++) {
		leaf->values[i] = temp_pointers[i];
		leaf->keys[i] = temp_keys[i];
		leaf->nkeys++;
	}

	for (int i = split, j = 0; i < ORDER; i++, j++) {
		new_leaf->values[j] = temp_pointers[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->nkeys++;
	}

	free(temp_pointers);
	free(temp_keys);

	new_leaf->values[ORDER - 1] = leaf->values[ORDER - 1];
	leaf->values[ORDER - 1] = (void*) new_leaf;

	for (int i = leaf->nkeys; i < ORDER - 1; i++)
		leaf->values[i] = NULL;
	for (int i = new_leaf->nkeys; i < ORDER - 1; i++)
		new_leaf->values[i] = NULL;

	new_leaf->parent = leaf->parent;
	int new_key = new_leaf->keys[0];

	return insert_into_parent(root, leaf, new_key, new_leaf);
}


// insert without causing a split
node* insert_into_node(node* root, node* parent, int left_index, int key, node* right) {
	for (int i = parent->nkeys; i > left_index; i--) {
		parent->values[i + 1] = parent->values[i];
		parent->keys[i] = parent->keys[i - 1];
	}
	parent->values[left_index + 1] = right;
	parent->keys[left_index] = key;
	parent->nkeys++;
	return root;
}


// insert so as to split in a node
node* insert_into_node_after_splitting(node* root, node* old_node, int left_index, int key, node* right) {
	node** temp_pointers = malloc((ORDER + 1) * sizeof(node*));
	int* temp_keys = malloc(ORDER * sizeof(int));
	int i, j;
	for (i = 0, j = 0; i < old_node->nkeys + 1; i++, j++) {
		if (j == left_index + 1) j++;
		temp_pointers[j] = old_node->values[i];
	}

	for (i = 0, j = 0; i < old_node->nkeys; i++, j++) {
		if (j == left_index) j++;
		temp_keys[j] = old_node->keys[i];
	}

	temp_pointers[left_index + 1] = right;
	temp_keys[left_index] = key;

	int split = find_break(ORDER);
	node* new_node = make_node();
	old_node->nkeys = 0;
	for (i = 0; i < split - 1; i++) {
		old_node->values[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->nkeys++;
	}
	old_node->values[i] = temp_pointers[i];
	int kp = temp_keys[split - 1];
	for (++i, j = 0; i < ORDER; i++, j++) {
		new_node->values[j] = temp_pointers[i];
		new_node->keys[j] = temp_keys[i];
		new_node->nkeys++;
	}
	new_node->values[j] = temp_pointers[i];
	free(temp_pointers);
	free(temp_keys);
	node* child = NULL;
	new_node->parent = old_node->parent;
	for (i = 0; i <= new_node->nkeys; i++) {
		child = new_node->values[i];
		child->parent = new_node;
	}

	return insert_into_parent(root, old_node, kp, new_node);
}



// insert new node (internal or leaf) into tree
node* insert_into_parent(node* root, node* left, int key, node* right) {
	node* parent = left->parent;
	if (parent == NULL)
		return insert_into_new_root(left, key, right);

	int left_index = 0;
	while (left_index <= parent->nkeys && parent->values[left_index] != left)
		left_index++;

	if (parent->nkeys < ORDER - 1)
		return insert_into_node(root, parent, left_index, key, right);

	return insert_into_node_after_splitting(root, parent, left_index, key, right);
}


// make new root for 2 subtrees, insert key into appropriate one
node* insert_into_new_root(node* left, int key, node* right) {
	node* root = make_node();
	root->keys[0] = key;
	root->values[0] = left;
	root->values[1] = right;
	root->nkeys++;
	root->parent = NULL;
	left->parent = root;
	right->parent = root;
	return root;
}


// brand new tree, returns new root
node* start_new_tree(int key, void* value) {
	node* root = make_leaf();
	root->keys[0] = key;
	root->values[0] = value;
	root->values[ORDER - 1] = NULL;
	root->parent = NULL;
	root->nkeys++;
	return root;
}



// general insertion -- ensures B+tree invariants are maintained via 
// above helper functions, duplicates are allowed
node* insert(node* root, int key, int value) {
	void* vvalue = (void*) value;
	if (root == NULL) 
		return start_new_tree(key, vvalue);

	node* leaf = find_leaf_first(root, key);
	if (leaf->nkeys < ORDER - 1) {
		leaf = insert_into_leaf(leaf, key, vvalue);
		return root;
	}
	else
		return insert_into_leaf_after_splitting(root, leaf, key, vvalue);
}



// remove single entry from a node, update other entries appropriately 
node* remove_entry_from_node(node* n, int key, node* value) {
	int i = 0;
	while (n->keys[i] != key && n->values[i] != value)
		i++;
	i++;
	for (; i < n->nkeys; i++)
		n->keys[i - 1] = n->keys[i];

	int num_pointers;
	i = 0;
	if (n->is_leaf) {
		num_pointers = n->nkeys;
		while (n->keys[i] != key && n->values[i] != value)
			i++;
	}
	else {
		num_pointers = n->nkeys + 1;
		while (n->values[i] != value)
			i++;
	}
	i++;
	for (; i < num_pointers; i++)
		n->values[i - 1] = n->values[i];
	n->nkeys--;

	if (n->is_leaf) {
		for (i = n->nkeys; i < ORDER - 1; i++)
			n->values[i] = NULL;
	}
	else {
		for (i = n->nkeys + 1; i < ORDER; i++)
			n->values[i] = NULL;
	}

	return n;
}


// update root if the root is now empty by promoting children
node* adjust_root(node* root) {
	if (root->nkeys > 0)
		return root;

	node* new_root;
	if (!root->is_leaf) {
		new_root = root->values[0];
		new_root->parent = NULL;
	}

	else
		new_root = NULL;

	free(root);

	return new_root;
}


// merge an undersized node with a neighbor
node* merge_nodes(node* root, node* n, node* neighbor, int neighbor_index, int kp) {
	node* tmp = NULL;
	if (neighbor_index == -1) {
		tmp = n;
		n = neighbor;
		neighbor = tmp;
	}
	int neighbor_insertion_index = neighbor->nkeys;
	int i, j;
	if (!n->is_leaf) {
		neighbor->keys[neighbor_insertion_index] = kp;
		neighbor->nkeys++;

		int n_end = n->nkeys;
		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->values[i] = n->values[j];
			neighbor->nkeys++;
			n->nkeys--;
		}

		neighbor->values[i] = n->values[j];

		for (i = 0; i < neighbor->nkeys + 1; i++) {
			tmp = (node *)neighbor->values[i];
			tmp->parent = neighbor;
		}
	}

	else {
		for (int i = neighbor_insertion_index, j = 0; j < n->nkeys; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->values[i] = n->values[j];
			neighbor->nkeys++;
		}
		neighbor->values[ORDER - 1] = n->values[ORDER - 1];
	}

	root = delete_entry(root, n->parent, kp, n);
	free(n); 
	return root;
}


// redistribute if no neighbor is small enough to merge without having
// to immediately then split
node* redistribute_nodes(node* root, node* n, node* neighbor, int neighbor_index, int kp_index, int kp) {  
	node* tmp = NULL;
	int i;
	if (neighbor_index != -1) {
		if (!n->is_leaf)
			n->values[n->nkeys + 1] = n->values[n->nkeys];
		for (i = n->nkeys; i > 0; i--) {
			n->keys[i] = n->keys[i - 1];
			n->values[i] = n->values[i - 1];
		}
		if (!n->is_leaf) {
			n->values[0] = neighbor->values[neighbor->nkeys];
			tmp = (node*) n->values[0];
			tmp->parent = n;
			neighbor->values[neighbor->nkeys] = NULL;
			n->keys[0] = kp;
			n->parent->keys[kp_index] = neighbor->keys[neighbor->nkeys - 1];
		}
		else {
			n->values[0] = neighbor->values[neighbor->nkeys - 1];
			neighbor->values[neighbor->nkeys - 1] = NULL;
			n->keys[0] = neighbor->keys[neighbor->nkeys - 1];
			n->parent->keys[kp_index] = n->keys[0];
		}
	}

	else {  
		if (n->is_leaf) {
			n->keys[n->nkeys] = neighbor->keys[0];
			n->values[n->nkeys] = neighbor->values[0];
			n->parent->keys[kp_index] = neighbor->keys[1];
		}
		else {
			n->keys[n->nkeys] = kp;
			n->values[n->nkeys + 1] = neighbor->values[0];
			tmp = (node*) n->values[n->nkeys + 1];
			tmp->parent = n;
			n->parent->keys[kp_index] = neighbor->keys[0];
		}
		for (i = 0; i < neighbor->nkeys - 1; i++) {
			neighbor->keys[i] = neighbor->keys[i + 1];
			neighbor->values[i] = neighbor->values[i + 1];
		}
		if (!n->is_leaf)
			neighbor->values[i] = neighbor->values[i + 1];
	}

	n->nkeys++;
	neighbor->nkeys--;

	return root;
}


// remove a key/value pair on a specific node,
// making updates as needed to maintain invariants
node* delete_entry(node* root, node* n, int key, void* value ) {
	n = remove_entry_from_node(n, key, value);
	if (n == root) 
		return adjust_root(root);

	int min_keys = n->is_leaf ? find_break(ORDER - 1) : find_break(ORDER) - 1;

	if (n->nkeys >= min_keys)
		return root;

	int neighbor_index = 0;
	for (; neighbor_index <= n->parent->nkeys; neighbor_index++) {
		if (n->parent->values[neighbor_index] == n) {
			neighbor_index--;
			break;
		}
	}

	int kp_index = neighbor_index == -1 ? 0 : neighbor_index;
	int kp = n->parent->keys[kp_index];
	node* neighbor = neighbor_index == -1 ? n->parent->values[1] : n->parent->values[neighbor_index];

	int capacity = n->is_leaf ? ORDER : ORDER - 1;

	if (neighbor->nkeys + n->nkeys < capacity)
		return merge_nodes(root, n, neighbor, neighbor_index, kp);

	else
		return redistribute_nodes(root, n, neighbor, neighbor_index, kp_index, kp);
}



// remove a specific key and update to maintain invariants
node* delete(node* root, int key, void* value) {
	node* key_leaf = find_leaf(root, key, value);
	if (key_leaf != NULL)
		root = delete_entry(root, key_leaf, key, (int*) value);
	return root;
}

// free all nodes in a tree
void destroy_tree(node* root) {
    if (!root)
        return;
	if (!root->is_leaf) {
		for (int i = 0; i < root->nkeys + 1; i++)
			destroy_tree(root->values[i]);
	}
	free(root);
	return;
}


int path_to_root( node * root, node * child ) {
	int length = 0;
	node * c = child;
	while (c != root) {
		c = c->parent;
		length++;
	}
	return length;
}


node* bulk_load(pair* data, int len, int (*compare)(const void *, const void *)) {
	if (len == 0) {
	    node* root = make_node();
	    root->is_leaf = 1;
	    return root;
    }
	    
	qsort(data, len, sizeof(pair), compare);

	// make leaves
	int nleafs = len / (ORDER - 1);
	node* start = make_node();
	node* current = start;
	int index = 0;
	int node_count = 0;
	for (int i = 0; i < nleafs; i++) {
	    node_count++;
		current->is_leaf = 1;
		current->nkeys = ORDER - 1;
		for (int j = 0; j < ORDER - 1; j++) {
			current->keys[j] = data[index].key;
			current->values[j] = data[index].value;
			index++;
		}
		if (i < nleafs - 1) {
			node* next = make_node();
			current->values[ORDER - 1] = next;
			current->right = next;
			current = next;
		}
	}
	int rest = len % (ORDER - 1);
	if (rest > 0) {
	    node* last = current;
	    if (node_count > 0) {
		    last = make_node();
		    node_count++;
		    current->values[ORDER - 1] = last;
		    current->right = last;
	    }
		    
		last->is_leaf = 1;
		last->nkeys = rest;
		for (int j = 0; j < rest; j++) {
			last->keys[j] = data[index].key;
			last->values[j] = data[index].value;
			index++;
		}
	}
	current->values[ORDER - 1] = NULL;


	// build layers of indices on top until we have just one node
	// at a level, a.k.a., the root
	while (node_count > 1) {
		node* new_start = make_node();
		int new_node_count = 0;
		current = new_start;
		int nnodes = node_count / (ORDER);
		rest = node_count % ORDER;
		int under_fill = (rest == 1) ? 1 : 0;
		for (int i = 0; i < nnodes; i += 1) {
		    new_node_count++;
			int j = 0;
			current->values[0] = start;
			start->parent = current;
			int lim = (i == nnodes - 1 && under_fill) ? ORDER - 2 : ORDER - 1;
			for (; j < lim; j++) {
			    start = start->right;
				current->keys[j] = start->keys[0];
				start->parent = current;
				current->values[j + 1] = start;
			}
			current->nkeys = (under_fill) ? ORDER - 2 : ORDER - 1;
			if (i < nnodes - 1) {
			    node* next = make_node();
			    current->right = next;
			    current = next;
		    }
    		start = start->right;
		}
		if (under_fill)
		    rest++;
		if (rest > 0) {
		    rest--;
		    if (new_node_count > 0) {
	            node* new_node = make_node();
	            current->right = new_node;
	            current = new_node;
		    }
	        new_node_count++;
		    current->nkeys = rest;
		    current->values[0] = start;
		    start->parent = current;
		    for (int j = 0; j < rest; j++) {
                start = start->right;
                start->parent = current;
			    current->keys[j] = start->keys[0];
			    current->values[j + 1] = start;
			    
		    }
		}
		node_count = new_node_count;
		start = new_start;
	}
	return start;
}
