#include "bplus_file_funcs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CALL_BF(call)         \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK)        \
		{                         \
			BF_PrintError(code);    \
			return code;     \
		}                         \
	}

// CALL_BF and destroy on failure
#define CALL_BF_D(call, block)         \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK)        \
		{                         \
			BF_Block_Destroy(&block); \
			BF_PrintError(code);    \
			return code;     \
		}                         \
	}

// CALL BF and unpin and destroy on failure
#define CALL_BF_U_D(call, block)         \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK)        \
		{                         \
			BF_UnpinBlock(block); \
			BF_Block_Destroy(&block); \
			BF_PrintError(code);    \
			return code;     \
		}                         \
	}

// CALL BF and close file and destroy block on failure
#define CALL_BF_C_D(call, fd, block)         \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK)        \
		{                         \
			BF_CloseFile(fd); \
			BF_Block_Destroy(&block); \
			BF_PrintError(code);    \
			return code;     \
		}                         \
	}
#define BPLUS_MAGIC_NUMBER 1357


int bplus_create_file(const TableSchema *schema, const char *fileName)
{
	int fd; // Aναγνωριστικό αρχείου
	BF_Block *block; // Δείκτης το block 0 που θα δημιουργηθεί
	BF_Block_Init(&block);

	CALL_BF_D(BF_CreateFile(fileName), block); // Δημηουργία αρχείου
	CALL_BF_D(BF_OpenFile(fileName, &fd), block); // Άνοιγμα του αρχείου που δημιουργήσαμε
	CALL_BF_C_D(BF_AllocateBlock(fd, block), fd, block); // Δέσμευση χώρου για το block 0

	BPlusMeta* meta = (BPlusMeta*)BF_Block_GetData(block); // Παίρνουμε την διεύθυνση της μνήμης των δεδομένων του block 0 (ως struct b tree metadata) 
	
	meta->magic_number = BPLUS_MAGIC_NUMBER;
	meta->max_records = (BF_BLOCK_SIZE-sizeof(LeafHeader))/sizeof(Record);
	meta->max_keys = (BF_BLOCK_SIZE-sizeof(IndexHeader)-sizeof(int))/(2*sizeof(int)); // Αφαιρούμε και τον πρώτο (έξτρα) δείκτη που υπάρχει στον index node ώστε να μείνουν μόνο ζευγάρια keys-pointers
	meta->index_flag = 'I';
	meta->leaf_flag = 'L';
	meta->root_block_id = -1; // Δεν υπαρχει ακομα ριζα
	meta->schema = *schema;

	BF_Block_SetDirty(block);
	CALL_BF_C_D(BF_UnpinBlock(block), fd, block);
	BF_CloseFile(fd);
	BF_Block_Destroy(&block);
	
	return 0;
}


int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata)
{
	BF_Block *block;
	BF_Block_Init(&block);
	
	// Ανοιγμα αρχειου
	CALL_BF_D(BF_OpenFile(fileName, file_desc), block);

	// Παίρνουμε το block 0
	CALL_BF_C_D(BF_GetBlock(*file_desc, 0, block), *file_desc, block);

	BPlusMeta* meta = (BPlusMeta*)BF_Block_GetData(block);

	
	if (meta->magic_number != BPLUS_MAGIC_NUMBER){
		fprintf(stderr, "Error: Invalid file format.\n");
		BF_UnpinBlock(block);
		BF_CloseFile(*file_desc);
		BF_Block_Destroy(&block);
		return -1;
	}

	// Δεσμεύουμε μνημη
	*metadata = malloc(sizeof(BPlusMeta));
	if (*metadata == NULL){
		fprintf(stderr, "Error allocating memory for metadata.\n");
		BF_UnpinBlock(block);
		BF_CloseFile(*file_desc);
		BF_Block_Destroy(&block);
		return -1;
	}

	// ώωζουμε τα δεδομενα στην δικη μας μνημη πριν κλεισουμε το block
	memcpy(*metadata, meta, sizeof(BPlusMeta));

	BF_UnpinBlock(block);
	BF_Block_Destroy(&block);
	return 0;
}


int bplus_close_file(const int file_desc, BPlusMeta* metadata)
{
	CALL_BF(BF_CloseFile(file_desc));
	free(metadata);
	return 0;
}


int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record)
{
	// Δημιουργία της ρίζας αμα δεν υπάρχει.
	if(metadata->root_block_id == -1){
		// Η ρίζα είναι αρχικά ένα φύλλο.
		int rootID;
		if (createLeafBlock(file_desc, metadata, &rootID) == -1){
			fprintf(stderr, "Error creating root leaf block.\n");
			return -1;
		}

		// Ενημέρωση BPlusMeta block 0 με το νέο rootID.
		updateRootID(file_desc, metadata, rootID);
	}



	// Βρίσκουμε το leaf block στο οποίο πρέπει να μπεί η εγγραφή.
	int leafID;
	int recordKey = record_get_key(&metadata->schema, record);
	if (findLeafBlockID(file_desc, metadata, recordKey, &leafID) == -1){
		fprintf(stderr, "Error finding corresponding leaf block to insert record.\n");
		return -1;
	}
	// Και τον πατέρα του στην περίπτωση που χρειαστεί να γίνει split.
	int parentID;
	if (findParentID(file_desc, metadata, leafID, recordKey, &parentID) == -1){
		fprintf(stderr, "Error finding parent.\n");
		return -1;
	}




	
	int recordsCount = getLeafRecordCount(file_desc, leafID);
	if (recordsCount == -1){
		fprintf(stderr, "Error fetching leaf record count.\n");
		return -1;
	}
	if (recordsCount != metadata->max_records){ // Το βάζουμε στο φύλλο αυτό άμα χωράει.
		if (insertLeafSorted(file_desc, metadata, leafID, record) == -1){
			fprintf(stderr, "Error inserting record into leaf.\n");
			return -1;
		}
	}else{ // Δεν χωράει οπότε πρέπει να γίνει split.
		
		int midKey, newLeafID;
		if (insertAndSplitLeafNode(file_desc, metadata, leafID, record, &midKey, &newLeafID) == -1){
			fprintf(stderr, "Error inserting and splitting leaf block.\n");
			return -1;
		}

		if (parentID == 0){ // Άμα η ρίζα είναι φύλλο τότε δημιουργούμε αμέσως μία νέα ρίζα πάνω που θα είναι index.
			if (createRootIndexBlock(file_desc, metadata, leafID, midKey, newLeafID) == -1){
				fprintf(stderr, "Error creating new root block.\n");
				return -1;
			}
			return 0;
		}
		
		// Αλλιώς αναδρομικά εισάγουμε στους index nodes πάνω μέχρι να χωρέσει.
		if (recursivelyInsertAndSplit(file_desc, metadata, parentID, midKey, newLeafID) == -1){
			fprintf(stderr, "Error adding keys recursively into index nodes.\n");
			return -1;
		}
	}
	
	return 0;
}


int bplus_record_find(const int file_desc, const BPlusMeta *metadata, const int key, Record** out_record)
{ 
	*out_record = NULL;
	// Βρίσκουμε το leaf ID στο οποίο θα έπρεπε να υπάρχει αυτό το κλειδί κανονικά.
	int leafID;
	if (findLeafBlockID(file_desc, metadata, key, &leafID) == -1){
		fprintf(stderr, "Error finding corresponding leaf block.\n");
		return -1;
	}
	
	BF_Block* leafBlock;
	BF_Block_Init(&leafBlock);
	CALL_BF_D(BF_GetBlock(file_desc, leafID, leafBlock), leafBlock);
	LeafNode* node = (LeafNode*)BF_Block_GetData(leafBlock);
	
	// Βλέπουμε αν όντως υπάρχει εκεί.
	// Αν δεν υπάρχει εκεί τότε δεν γίνεται να βρίσκεται αλλού λόγω της λογικής του B-Tree (ταξινομημένα δεδομένα)
	for (int i = 0; i < node->header.count; i++){
		int currentKey = record_get_key(&metadata->schema, &node->records[i]);
		if (currentKey == key){ // Βρέθηκε
			*out_record = malloc(sizeof(Record));
			**out_record = node->records[i];
			CALL_BF_D(BF_UnpinBlock(leafBlock), leafBlock);
			BF_Block_Destroy(&leafBlock);
			return 0;
		}
	}
	CALL_BF_D(BF_UnpinBlock(leafBlock), leafBlock);
	BF_Block_Destroy(&leafBlock);
	return -1;
}

