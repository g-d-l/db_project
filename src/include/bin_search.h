// basic binary search over an array to find which index to insert an element
int bin_search_ins(int* array, int len, int val);

// basic binary search over an array to find which index to delete an element
int bin_search_del(int* array, int len, int val);

// return the last instance of val, len otherwise
int bin_search_high(int* array, int len, int val);

// return the index of the first instance of val, 0 otherwise
int bin_search_low(int* array, int len, int val);
