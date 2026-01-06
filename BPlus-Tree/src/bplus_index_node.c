#include "../include/bplus_file_funcs.h"
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


//////////////////////
// ΒΟΗΘΗΤΙΚΈΣ INDEX //
//////////////////////

// Δίνει πόσα κλειδιά έχει ένας index node με id = ID.
static int getKeysCount(int fd, int ID){
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF_D(BF_GetBlock(fd, ID, block), block);

    IndexNode* node = (IndexNode*)BF_Block_GetData(block);
    int count = node->header.keysCount;

    CALL_BF_D(BF_UnpinBlock(block), block);
    BF_Block_Destroy(&block);
    return count;
}

// Δημιουργεί νέο index block (node) και αποθηκεύει το ID του στο ID
// Επιστρέφει 0 για επιτυχία, -1 για αποτυχία
static int createIndexBlock(int fd, BPlusMeta* meta, int* ID){
    BF_Block *block;
    BF_Block_Init(&block);
    CALL_BF_D(BF_AllocateBlock(fd, block), block);

    IndexNode* node = (IndexNode*)BF_Block_GetData(block);
    node->header.type = meta->index_flag;
    node->header.keysCount = 0;

    BF_Block_SetDirty(block);
    BF_Block_Destroy(&block);

    CALL_BF(BF_GetBlockCounter(fd, ID));
    *ID = *ID - 1; // Χωρίς το Block 0
    return 0;
}

// Βάζει ένα key σε έναν index node και τον κρατάει ταξινομημένο.
static int insertIndexSorted(int fd, BPlusMeta* meta, int indexID, int key, int pointerID){
    // Ελέγχουμε ότι όντως είναι index block
    BF_Block* indexBlock;
    BF_Block_Init(&indexBlock);
    CALL_BF_D(BF_GetBlock(fd, indexID, indexBlock), indexBlock);

    char* data = BF_Block_GetData(indexBlock);
    if (data[0] != meta->index_flag){
        CALL_BF_D(BF_UnpinBlock(indexBlock), indexBlock);
        BF_Block_Destroy(&indexBlock);
        return -1;
    }
    IndexNode* node = (IndexNode*)data;

    // Ελέγχουμε ότι όντως χωράνε κλειδιά
    if (node->header.keysCount == meta->max_keys){
        CALL_BF_D(BF_UnpinBlock(indexBlock), indexBlock);
        BF_Block_Destroy(&indexBlock);
        return -1; 
    }

    // Insert sort
    // Μεταφέρουμε όλα τα μεγαλύτερα κλειδιά (μαζί με τον δείκτη τους) του πίνακα μία θέση δεξιά
    // Όταν βρούμε το πρώτο κλειδί του πίνακα που είναι μικρότερο, το βάζουμε στην νέα θέση (δεξιά του) που έχει δημιουργηθεί μαζί με τον δείκτη του
    int i = 2 * node->header.keysCount - 1; // Διατρέχουμε τον πίνακα απο το τελευταίο κλειδί του πίνακα (μόνο τα κλειδιά)
    while (i >= 0){
        int currentKey = node->keys_pointers[i];

        if (currentKey > key){
            node->keys_pointers[i+2] = node->keys_pointers[i]; // Το κλειδί
            node->keys_pointers[i+3] = node->keys_pointers[i+1]; // Ο δείκτης
            i -= 2;
        }else if (currentKey == key){ // Απαγορεύονται οι διπλότυπες εγγραφές
            fprintf(stderr, "Error: Duplicates are not allowed.\n");
            CALL_BF_D(BF_UnpinBlock(indexBlock), indexBlock);
            BF_Block_Destroy(&indexBlock);
            return -1;
        }else{ // Βρήκαμε την θέση
            break;
        }
    }

    // Τα βάζουμε στην σωστή θέση.
    node->keys_pointers[i+2] = key;
    node->keys_pointers[i+3] = pointerID;
    node->header.keysCount++;

    // Ενημέρωση.
    BF_Block_SetDirty(indexBlock);
    CALL_BF_D(BF_UnpinBlock(indexBlock), indexBlock);
    BF_Block_Destroy(&indexBlock);
    return 0;
}

// Παρόμοια λογική με την insertAndSplitLeafNode
static int insertAndSplitIndexNode(int fd, BPlusMeta* meta, int indexID, int key, int pointer, int* midKey, int* newIndexID){
    // Ελέγχουμε ότι όντως είναι index block
    BF_Block* indexBlock;
    BF_Block_Init(&indexBlock);
    CALL_BF_D(BF_GetBlock(fd, indexID, indexBlock), indexBlock);
    char* data = BF_Block_GetData(indexBlock);
    if (data[0] != meta->index_flag){
        CALL_BF_D(BF_UnpinBlock(indexBlock), indexBlock);
        BF_Block_Destroy(&indexBlock);
        return -1;
    }
    IndexNode* node = (IndexNode*)data;



    // Εδώ, αντι για ένα στοιχείο με τα φύλλα, αντιγράφουμε ζευγάρια κλειδιών και δεικτών κάθε φορά
    int tempSize = 2*(node->header.keysCount) + 1 + 2;
    int temp_keys_pointers[tempSize];

    bool foundSpot = false;
    int j = 1;
    int currentKey;
    temp_keys_pointers[0] = node->keys_pointers[0]; // O αριστερός δείκτης θα παραμείνει ίδιος
    for (int i = 1; i <= tempSize - 2; i += 2){
        if (j >= 2*node->header.keysCount){
            temp_keys_pointers[i] = key;
            temp_keys_pointers[i+1] = pointer;
            break;
        }
        currentKey = node->keys_pointers[j];

        if (foundSpot || currentKey < key){ 
            temp_keys_pointers[i] = node->keys_pointers[j];
            temp_keys_pointers[i+1] = node->keys_pointers[j+1];
            j += 2;
        }else if (currentKey == key){
            CALL_BF_D(BF_UnpinBlock(indexBlock), indexBlock);
            BF_Block_Destroy(&indexBlock);
            return -1;
        }else{
            temp_keys_pointers[i] = key;
            temp_keys_pointers[i+1] = pointer;
            foundSpot = true;
        }
    }

    // Φτιάχνουμε το δεύτερο φύλλο.
    if (createIndexBlock(fd, meta, newIndexID) == -1){
        return -1;
    }
    BF_Block* newIndexBlock;
    BF_Block_Init(&newIndexBlock);
    CALL_BF_D(BF_GetBlock(fd, *newIndexID, newIndexBlock), newIndexBlock);
    IndexNode* newNode = (IndexNode*)BF_Block_GetData(newIndexBlock);



    // Oι δείκτες και τα κλειδιά μέχρι την μέση παραμένουν στο αριστερό (αρχικό) φύλλο ενώ οι υπόλοιπες (ΧΩΡΊΣ το μεσαίο κλειδί) πάνε στο δεξί (νέο) φύλλο.
    int midIndex = (tempSize) / 2;
    *midKey = temp_keys_pointers[midIndex];

    node->header.keysCount = 0;
    for (int i = 0; i < midIndex; i++){
        node->keys_pointers[i] = temp_keys_pointers[i];
        if (i % 2 == 1) node->header.keysCount++; // Στις μονές θέσεις είναι τα κλειδιά οπότε τότε μόνο αυξάνουμε το πλήθος.
    }

    newNode->header.keysCount = 0;
    j = 0;
    for (int i = midIndex + 1; i <= tempSize - 1; i++){
        newNode->keys_pointers[j] = temp_keys_pointers[i];
        if (j % 2 == 1) newNode->header.keysCount++;
        j++;
    }

    // Ενημέρωση και στα δύο
    BF_Block_SetDirty(indexBlock);
    BF_Block_SetDirty(newIndexBlock);
    CALL_BF_D(BF_UnpinBlock(indexBlock), indexBlock);
    CALL_BF_D(BF_UnpinBlock(newIndexBlock), newIndexBlock);
    BF_Block_Destroy(&indexBlock);
    BF_Block_Destroy(&newIndexBlock);
    return 0;
}


///////////////////////
//   ΔΗΜΟΣΙΕΣ INDEX  //
///////////////////////


int updateRootID(int fd, BPlusMeta* metadata, int newID){
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF_D(BF_GetBlock(fd, 0, block), block);

    BPlusMeta* meta = (BPlusMeta*)BF_Block_GetData(block);
    meta->root_block_id = newID;
    metadata->root_block_id = newID;

    BF_Block_SetDirty(block);
    CALL_BF_D(BF_UnpinBlock(block), block);
    BF_Block_Destroy(&block);
    return 0;
}

int findParentID(int fd, BPlusMeta* meta, int childID, int key, int* parentID){
    BF_Block* block;
    BF_Block_Init(&block);

    // "Πατέρας" της ρίζας το block 0
    if (childID == meta->root_block_id){
        *parentID = 0;
        BF_Block_Destroy(&block);
        return 0;
    }

    int currentBlockID = meta->root_block_id; 
    CALL_BF_D(BF_GetBlock(fd, currentBlockID, block), block);
    char* data = BF_Block_GetData(block);
    
    // Διατρέχουμε αρχικά όλα τα index nodes μέχρι να το βρούμε το κλειδί.
    while (data[0] != meta->leaf_flag){
        IndexNode* node = (IndexNode*)data;
        if (node->header.keysCount == 0){ // Ελέγχουμε ότι σίγουρα έχει κλειδιά το index για να μην μπούμε σε ατέρμον βρόχο.
            CALL_BF_D(BF_UnpinBlock(block), block);
            BF_Block_Destroy(&block);
            return -1;
        }

        // Ψάχνουμε αν στο index block αυτό υπάρχει κάποιος δείκτης που δείχνει στο ID που θέλουμε (childID)
        int lastPointerIndex = 2 * node->header.keysCount;
        for (int i = 0; i <= lastPointerIndex; i +=2){
            if (node->keys_pointers[i] == childID){
                *parentID = currentBlockID;
                CALL_BF_D(BF_UnpinBlock(block), block);
                BF_Block_Destroy(&block);
                return 0;
            }
        }

        // Αν όχι βλέπουμε προς τα που πρέπει να πάμε μετά με βάση το κλειδί
        int lastKeyIndex = lastPointerIndex - 1; 
        for (int i = 1; i <= lastKeyIndex; i += 2){ // Διατρέχουμε όλα τα κλειδιά.
            if (node->keys_pointers[i] > key){
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

    // Φτάσαμε σε φύλλο οπότε δεν βρήκαμε το ID
    CALL_BF_D(BF_UnpinBlock(block), block); // Unpin το φύλλο.
    BF_Block_Destroy(&block);
    return -1; 
}

int createRootIndexBlock(int fd, BPlusMeta* meta, int leftPointerID, int midKey, int rightPointerID){
    int rootID;
    if (createIndexBlock(fd, meta, &rootID) == -1){
        return -1;
    }
    BF_Block* rootBlock;
    BF_Block_Init(&rootBlock);
    CALL_BF_D(BF_GetBlock(fd, rootID, rootBlock), rootBlock);
    IndexNode* rootNode = (IndexNode*)BF_Block_GetData(rootBlock);

    rootNode->keys_pointers[0] = leftPointerID;
    rootNode->keys_pointers[1] = midKey;
    rootNode->keys_pointers[2] = rightPointerID;
    rootNode->header.keysCount++;
    
    if (updateRootID(fd, meta, rootID) == -1){
        BF_Block_SetDirty(rootBlock);
        CALL_BF_D(BF_UnpinBlock(rootBlock), rootBlock);
        BF_Block_Destroy(&rootBlock);
        return -1;
    }

    BF_Block_SetDirty(rootBlock);
    CALL_BF_D(BF_UnpinBlock(rootBlock), rootBlock);
    BF_Block_Destroy(&rootBlock);
    return 0;
}

int recursivelyInsertAndSplit(int fd, BPlusMeta* meta, int indexID, int midKey, int rightID){
    // Συνθήκη τερματισμού 1: Βρήκαμε index node που έχει χώρο
    int keysCount = getKeysCount(fd, indexID);
    if (keysCount == -1){
        return -1;
    }
    if (keysCount < meta->max_keys){
        insertIndexSorted(fd, meta, indexID, midKey, rightID);
        return 0;
    }

    
    // Δεν χωράει σε αυτόν τον index node οπότε πρέπει να γίνει split.
    // Βρίσκουμε αρχικά τον πατέρα του node αυτού.
    int parentID;
    if (findParentID(fd, meta, indexID, midKey, &parentID) == -1){
        return -1;
    }
    
    // Τον σπάμε βάζοντας το midKey και rightID που πρέπει.
    int newMidKey, newRightID;
    if (insertAndSplitIndexNode(fd, meta, indexID, midKey, rightID, &newMidKey, &newRightID) == -1){
        return -1;
    }
    
    // Παίρνουμε όμως καινούργια newMidKey και newRightID που πρέπει να τα προωθήσουμε προς τα πάνω.
    // Συνθήκη τερματισμόυ 2: Άμα δεν υπάρχει άλλος parent index node πάνω τότε πρέπει να δημιουργήσουμε μία νέα ρίζα. Βάζουμε εδώ αυτήν την συνθήκη αντί της αρχής για να μπορέσουμε να περάσουμε τον αριστερό δείκτη εύκολα.
    if (parentID == 0){
        if (createRootIndexBlock(fd, meta, indexID, newMidKey, newRightID) == -1){ // Το indexID θα είναι πάντα ο αριστερός κόμβος.
            return -1;
        }
        return 0;
    }else{ // Αλλιώς αναδρομικά στον ήδη υπάρχον parent index node.
        return recursivelyInsertAndSplit(fd, meta, parentID, newMidKey, newRightID);
    }
    
    return 0;
}