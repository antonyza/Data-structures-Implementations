#ifndef HP_FILE_STRUCTS_H
#define HP_FILE_STRUCTS_H

#include <record.h>

/**
 * @file hp_file_structs.h
 * @brief Data structures for heap file management
 */

/* -------------------------------------------------------------------------- */
/*                              Data Structures                               */
/* -------------------------------------------------------------------------- */

/**
 * @brief Heap file header containing metadata about the file organization
 */
typedef struct HeapFileHeader {
    char *fileType; // "HP" για header file
    int blockCount; // Αριθμός block στο αρχείο
} HeapFileHeader;

/**
 * @brief Block header containing metadata about a block
 */
typedef struct BlockHeader {
    int recordsCount; // Πόσες εγγραφές υπάρχουν ήδη στο block
} BlockHeader;

/**
 * @brief Iterator for scanning through records in a heap file
 */
typedef struct HeapFileIterator {
    int file_handle; // Ποιο αρχείο
    int searchID; // Το ζητούμενο ID για το φιλτράρισμα
    int currentRecord; // Τρέχουσα εγγραφή
    int currentBlock; // Τρέχον block
    BF_Block *block; // Ο δείκτης στο τρέχον block 
} HeapFileIterator;

#endif /* HP_FILE_STRUCTS_H */
