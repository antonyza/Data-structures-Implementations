//
// Created by theofilos on 11/4/25.
//

#ifndef BPLUS_BPLUS_FILE_STRUCTS_H
#define BPLUS_BPLUS_FILE_STRUCTS_H
#include "bf.h"
#include "record.h"

typedef struct {
    int magic_number;
    TableSchema schema;
    int root_block_id;
    char index_flag;
    char leaf_flag;
    int max_records;
    int max_keys;
} BPlusMeta;



typedef struct {
    char type;
    int keysCount; // Ποσα κλειδια εχει το block (index node)
}IndexHeader;

// Περίεχει ζευγάρια κλειδιών-δεικτών. Ένας έξτρα δείκτης στην αρχή. Δηλαδή ο πίνακας είναι Δ - Κ - Δ - Κ - Δ - ... - Δ.
// Μέγιστος χώρος του πίνακα αυτού: 2 * keysCount + 1.
// Τελευταίος δείκτης του πίνακα: 2 * keysCount.
// Τελευταίο κλειδι: 2 * keysCount - 1.
typedef struct{
    IndexHeader header;
    int keys_pointers[];
}IndexNode; 



typedef struct {
    char type;
    int count; // Ποσες εγγραφες εχει το block (leaf node)
    int next_leaf_id;
}LeafHeader;

typedef struct{
    LeafHeader header;
    Record records[];
}LeafNode;



#endif //BPLUS_BPLUS_FILE_STRUCTS_H


