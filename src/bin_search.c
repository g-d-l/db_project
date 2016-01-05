// find position for insertion
int bin_search_ins(int* array, int len, int val) {
    int low = 0;
    int high = len - 1;
    int middle = (low + high) / 2;
 
    while (low <= high) {
        if (array[middle] < val)
            low = middle + 1;    
        else if (array[middle] == val)
            return middle + 1;
        else
            high = middle - 1;
 
        middle = (low + high) / 2;
    } 
    
    return low;
}

// for deletion
int bin_search_del(int* array, int len, int val) {
    int low = 0;
    int high = len - 1;
    int middle = (low + high) / 2;
 
    while (low <= high) {
        if (array[middle] < val)
            low = middle + 1;    
        else if (array[middle] == val)
            return middle;
        else
            high = middle - 1;
 
        middle = (low + high) / 2;
    } 
    
    return low;
}

// return the last instance of val, len otherwise
int bin_search_high(int* array, int len, int val) {
    return bin_search_del(array, len, val);
}

// return the index of the first instance of val, 0 otherwise
int bin_search_low(int* array, int len, int val) {
    int low = 0;
    int high = len - 1;
    int middle = (low + high) / 2;
 
    while (low <= high) {
        if (array[middle] < val)
            low = middle + 1;    
        else if (array[middle] == val && (middle == 0 || array[middle - 1] < array[middle]))
            return middle;
        else
            high = middle - 1;
 
        middle = (low + high) / 2;
    } 
    
    return low;
}
