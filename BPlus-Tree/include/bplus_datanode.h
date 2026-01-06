#include "bplus_file_funcs.h"
#include "bplus_file_structs.h"

#ifndef BP_DATANODE_H
#define BP_DATANODE_H
/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/

/**
 * @brief Finds the leaf block (ID) that a record key should be in, whether it is there or not.
 * @param fd File descriptor of the file.
 * @param meta Pointer to the B-Tree Metadata.
 * @param recordKey The record key.
 * @param leafID Stores the leafID found.
 * @return 0 on success, -1 on failure.
 */
int findLeafBlockID(int fd, const BPlusMeta* meta, int recordKey, int* leafID);

/**
 * @brief Gets the records count of a leaf block with a given ID.
 * @param fd File descriptor of the file.
 * @param ID ID of the leaf block.
 * @return Records count (>=0) of leaf block. -1 on failure.
 */
int getLeafRecordCount(int fd, int ID);

/**
 * @brief Creates and initializes a new empty leaf block.
 * @param fd File descriptor of the file to create the leaf block.
 * @param meta Pointer to the B-Tree Metadata.
 * @param ID ID of the new leaf block created.
 * @return - on success, -1 on failure.
 */
int createLeafBlock(int fd, BPlusMeta* meta, int* ID);

/**
 * @brief Inserts a new record inside a leaf and keeps it sorted, if it's not full. Makes the block dirty if successful.
 * @param fd File descriptor of the file to insert the record
 * @param meta Pointer to the B-Tree Metadata.
 * @param leafID ID of the leaf block to insert.
 * @param record Record that should be inserted here.
 * @return 0 on success, -1 on failure.
 */
int insertLeafSorted(int fd, BPlusMeta* meta, int leafID, const Record* record);


/**
 * @brief Inserts a record into a full leaf and splits it into two.
 * @param fd File descriptor of the file to insert the record
 * @param meta Pointer to the B-Tree Metadata.
 * @param leafID ID of the leaf block to insert.
 * @param record The record that should be inserted here.
 * @param midKey The key of the middle record that should go upwards (towards an index).
 * @param newLeafID The ID of the new (right) leaf block created. The other one (left) is the leafID block.
 * @return 0 on success, -1 on failure.
 */
int insertAndSplitLeafNode(int fd, BPlusMeta* meta, int leafID, const Record* record, int* midKey, int* newLeafID);
#endif
