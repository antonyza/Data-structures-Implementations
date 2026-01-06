#include "bf.h"
#include "bplus_file_structs.h"
#include "bplus_datanode.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define CALL_BF(call)         \
	{                           \
		BF_ErrorCode code = call; \
		if (code != BF_OK)        \
		{                         \
			BF_PrintError(code);    \
			return -1;     \
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
			return -1;     \
		}                         \
	}

// Μπορείτε να προσθέσετε εδώ βοηθητικές συναρτήσεις για την επεξεργασία Κόμβων Δεδομένων.

// Ξεκινάμε από την ρίζα πηγαίνοντας στον κατάλληλο δείκτη (διάστημα τιμών) επόμενου block κάθε φορά μέχρι να βρούμε το φύλλο που ανήκει.
int findLeafBlockID(int fd, const BPlusMeta* meta, int recordKey, int* leafID){
	BF_Block* block;
	BF_Block_Init(&block);

	int currentBlockID = meta->root_block_id; // Αν η ρίζα είναι φύλλο την επιστρέφει σωστά.
	CALL_BF_D(BF_GetBlock(fd, currentBlockID, block), block);
	char* data = BF_Block_GetData(block);

    // Διατρέχουμε αρχικά όλα τα index nodes μέχρι να φτάσουμε σε φύλλο.
	while (data[0] != meta->leaf_flag){
		IndexNode* node = (IndexNode*)data;
        if (node->header.keysCount == 0 || node->header.keysCount > meta->max_keys){ // Ελέγχουμε ότι σίγουρα έχει κλειδιά το index.
            CALL_BF_D(BF_UnpinBlock(block), block);
            BF_Block_Destroy(&block);
            return -1;
        }

		int lastKeyIndex = 2 * node->header.keysCount - 1; 
		for (int i = 1; i <= lastKeyIndex; i += 2){ // Διατρέχουμε όλα τα κλειδιά.
			if (node->keys_pointers[i] > recordKey){
				currentBlockID = node->keys_pointers[i-1];
				break;
			}

			// Ξεχωριστή περίπτωση για τον τελευταίο (δεξί) δείκτη.
			if (i == lastKeyIndex){
				currentBlockID = node->keys_pointers[i+1];
			}
		}
		CALL_BF_D(BF_UnpinBlock(block), block); // Unpin το block που βλέπαμε τώρα.
		CALL_BF_D(BF_GetBlock(fd, currentBlockID, block), block); // Παίρνουμε το επόμενο.
		data = BF_Block_GetData(block);
	}

	// Βρήκαμε το φύλλο.
    *leafID = currentBlockID;
	CALL_BF_D(BF_UnpinBlock(block), block);
	BF_Block_Destroy(&block);
	return 0;
}

int getLeafRecordCount(int fd, int ID){
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF_D(BF_GetBlock(fd, ID, block), block);

    LeafNode* node = (LeafNode*)BF_Block_GetData(block);
    int count = node->header.count;

    CALL_BF_D(BF_UnpinBlock(block), block);
    BF_Block_Destroy(&block);
    return count; 
}

int createLeafBlock(int fd, BPlusMeta* meta, int* ID){
    BF_Block *block;
    BF_Block_Init(&block);
    CALL_BF_D(BF_AllocateBlock(fd, block), block);

    LeafNode* node = (LeafNode*)BF_Block_GetData(block);
    node->header.type = meta->leaf_flag;
    node->header.count = 0;
    node->header.next_leaf_id = -1;

    BF_Block_SetDirty(block);
    CALL_BF_D(BF_UnpinBlock(block), block);
    BF_Block_Destroy(&block);

    CALL_BF(BF_GetBlockCounter(fd, ID));
    *ID = *ID - 1; // Χωρίς το Block 0
    return 0;
}

int insertLeafSorted(int fd, BPlusMeta* meta, int leafID,const Record* record){
    // Ελέγχουμε ότι όντως είναι leaf block
    BF_Block* leafBlock;
    BF_Block_Init(&leafBlock);
    CALL_BF_D(BF_GetBlock(fd, leafID, leafBlock), leafBlock);

    char* data = BF_Block_GetData(leafBlock);
    if (data[0] != meta->leaf_flag){
        CALL_BF_D(BF_UnpinBlock(leafBlock), leafBlock);
        BF_Block_Destroy(&leafBlock);
        return -1;
    }
    LeafNode* node = (LeafNode*)data;

    // Ελέγχουμε ότι όντως χωράνε εγγραφές
    if (node->header.count == meta->max_records){
        CALL_BF_D(BF_UnpinBlock(leafBlock), leafBlock);
        BF_Block_Destroy(&leafBlock);
        return -1; 
    }
    
    // Insert sort
    // Μεταφέρουμε όλες τις μεγαλύτερες εγγραφές (με βάση τα κλειδιά τους) του πίνακα μία θέση δεξιά
    // Όταν βρούμε το πρώτο κλειδί του πίνακα που είναι μικρότερο, το βάζουμε στην νέα θέση (δεξιά του) που έχει δημιουργηθεί
    int recKey = record_get_key(&meta->schema, record);
    int i = node-> header.count - 1; // Διατρέχουμε τον πίνακα απο το τέλος
    while (i >= 0){
        int currentKey = record_get_key(&meta->schema, &node->records[i]);

        if (currentKey > recKey){
            node->records[i+1] = node->records[i];
            i--;
        }else if (currentKey == recKey){ // Απαγορεύονται οι διπλότυπες εγγραφές
            fprintf(stderr, "Error: Duplicates are not allowed.\n");
            CALL_BF_D(BF_UnpinBlock(leafBlock), leafBlock);
            BF_Block_Destroy(&leafBlock);
            return -1;
        }else{
            break;
        }
    }

    node->records[i+1] = *record;
    node->header.count++;
    
    BF_Block_SetDirty(leafBlock);
    CALL_BF_D(BF_UnpinBlock(leafBlock), leafBlock);
    BF_Block_Destroy(&leafBlock);
    return 0;
}

int insertAndSplitLeafNode(int fd, BPlusMeta* meta, int leafID,const Record* record, int* midKey, int* newLeafID){
    // Ελέγχουμε ότι όντως είναι leaf block
    BF_Block* leafBlock;
    BF_Block_Init(&leafBlock);
    CALL_BF_D(BF_GetBlock(fd, leafID, leafBlock), leafBlock);

    char* data = BF_Block_GetData(leafBlock);
    if (data[0] != meta->leaf_flag){ // Και τα δύο structs IndexNode και LeafNode έχουν ως πρώτη θέση μνήμης την μεταβλητή type.
        CALL_BF_D(BF_UnpinBlock(leafBlock), leafBlock);
        BF_Block_Destroy(&leafBlock);
        return -1;
    }
    LeafNode* node = (LeafNode*)data; // Είναι φύλλο σίγουρα.


    // Δημιουργούμε έναν βοηθητικό πίνακα όπου χωράει η εγγραφή που θέλουμε να εισάγουμε.
    // Θέλουμε αυτός να παραμείνει ταξινομημένος ώστε να εκτελέσουμε το split σωστα για να επιστρέψουμε την σωστή μεσαία τιμή πάνω.
    // Γι'αυτό αντιγράφουμε ένα προς ένα όλες τις εγγραφές του node μέχρι να βρούμε που πρέπει να μπει η ζητούμενη εγγραφή. Μετά ξανασυνεχίζουμε.
    int tempSize = node->header.count +1;
    Record temp_records[tempSize];
    int recKey = record_get_key(&meta->schema, record);

    bool foundSpot = false;
    int j = 0; // Για τον πίνακα του leaf node.
    int currentKey;
    for (int i = 0; i <= tempSize - 1; i++){
        if (j >= node->header.count) { // Για την περίπτωση όπου η ζητούμενη εγγραφή μπαίνει στο τέλος (το currentKey θα πάρει μια τιμή εκτός ορίων του πίνακα node->records)
            temp_records[i] = *record;
            break;
        }

        currentKey = record_get_key(&meta->schema, &node->records[j]);
        if (foundSpot || currentKey < recKey){
            temp_records[i] = node->records[j];
            j++;
        }else if (currentKey == recKey){
            fprintf(stderr, "Error: Duplicates are not allowed.\n");
            CALL_BF_D(BF_UnpinBlock(leafBlock), leafBlock);
            BF_Block_Destroy(&leafBlock);
            return -1;
        }else{
            temp_records[i] = *record;
            foundSpot = true;
        }
    }


    // Φτιάχνουμε το δεύτερο φύλλο.
    if (createLeafBlock(fd, meta, newLeafID) == -1){
        return -1;
    }
    BF_Block* newLeafBlock;
    BF_Block_Init(&newLeafBlock);
    CALL_BF_D(BF_GetBlock(fd, *newLeafID, newLeafBlock), newLeafBlock);
    LeafNode* newNode = (LeafNode*)BF_Block_GetData(newLeafBlock);

    // Το νέο φύλλο θα δείχνει όπου έδειχνε κανονικά το αρχικό φύλλο
    // Το αρχικό φύλλο θα δείχνει στο νέο φύλλο.
    // Τα προηγούμενα φύλλα θα δείχνουν κανονικά στο αριστερό (αρχικό) φύλλο.
    newNode->header.next_leaf_id = node->header.next_leaf_id;
    node->header.next_leaf_id = *newLeafID;



    // Οι εγγραφές μέχρι την μέση παραμένουν στο αριστερό (αρχικό) φύλλο ενώ οι υπόλοιπες (μαζί με το μεσαίο) πάνε στο δεξί (νέο) φύλλο.
    int midIndex = (tempSize + 1) / 2;
    Record midRecord = temp_records[midIndex];
    *midKey = record_get_key(&meta->schema, &midRecord);

    node->header.count = 0;
    for (int i = 0; i < midIndex; i++){
        node->records[i] = temp_records[i];
        node->header.count++;
    }

    newNode->header.count = 0;
    j = 0;
    for (int i = midIndex; i <= tempSize - 1; i++){
        newNode->records[j] = temp_records[i];
        newNode->header.count++;
        j++;
    }

    // Ενημέρωση και στα δύο
    BF_Block_SetDirty(leafBlock);
    BF_Block_SetDirty(newLeafBlock);
    CALL_BF_D(BF_UnpinBlock(leafBlock), leafBlock);
    CALL_BF_D(BF_UnpinBlock(newLeafBlock), newLeafBlock);
    BF_Block_Destroy(&leafBlock);
    BF_Block_Destroy(&newLeafBlock);
    return 0;
}