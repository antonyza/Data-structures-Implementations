#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file_structs.h"
#include "record.h"

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return 0;        \
    }                         \
  }

int HeapFile_Create(const char* fileName)
{
  // Φτιάχνουμε το αρχείο
  int file_handle;
  CALL_BF(BF_CreateFile(fileName)) // Δημιουργία αρχείου
  CALL_BF(BF_OpenFile(fileName, &file_handle)) // Άνοιγμα αρχείου στον file_handle

  // Αρχικοποίηση ενός block
  BF_Block *block;
  BF_Block_Init(&block);

  // Δεσμεύουμε το πρώτο block στο αρχείο που θα είναι η κεφαλίδα του heap file
  CALL_BF(BF_AllocateBlock(file_handle, block)); 

  // Φτιάχνουμε την κεφαλίδα
  HeapFileHeader header;
  header.fileType = "HP";
  header.blockCount = 1; // 1 block κατά την δημιουργία που είναι το header block

  // Εισάγουμε τα δεδομένα στo block
  char *data = BF_Block_GetData(block);
  memcpy(data, &header, sizeof(HeapFileHeader));
  BF_Block_SetDirty(block); // Dirty ώστε να γίνει ενημέρωση στον σκληρό δίσκο

  // Unpin και κλείσιμο
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  BF_CloseFile(file_handle);

  return 1;
}

int HeapFile_Open(const char *fileName, int *file_handle, HeapFileHeader** header_info)
{
  // Άνοιγμα του αρχείου με το δοσμένο όνομα στο δοσμένο file handle
  CALL_BF(BF_OpenFile(fileName, file_handle));

  // Αρχικοποίηση ενός block ώστε να διαβάσουμε την κεφαλίδα
  BF_Block *block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(*file_handle, 0, block));

  // Αντιγραφή της κεφαλίδας στο header_info για να το επιστρέψουμε
  *header_info = malloc(sizeof(HeapFileHeader)); // Δεσμεύουμε μνήμη για την κεφαλίδα που θα την επιστρέψουμε
  char *data = BF_Block_GetData(block);
  memcpy(*header_info, data, sizeof(HeapFileHeader));

  // Έλεγχος πως το δοσμένο αρχείο είναι heap file
  if (strcmp((*header_info)->fileType, "HP")){
    printf("Το αρχείο δεν είναι Heap File!\n");
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    CALL_BF(BF_CloseFile(*file_handle));
    return 0;
  }

  // Είναι Heap File άρα κανονικά unpin και destroy
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  return 1;
}

int HeapFile_Close(int file_handle, HeapFileHeader *hp_info)
{
  // Κλείνουμε το αρχείο
  CALL_BF(BF_CloseFile(file_handle));

  // Αποδεσμεύουμε την μνήμη που κάναμε malloc για την κεφαλίδα
  if (hp_info){
    free(hp_info);
  }

  return 1;
}

// Δημιουργεί ένα νέο Block και επιστρέφει την διευθυνσή του στην block
static int createNewBlock(int file_handle, HeapFileHeader *hp_info, BF_Block *block){
  char *data;
  
  // Δέσμευση νέου block
  CALL_BF(BF_AllocateBlock(file_handle, block));

  // Ενημέρωση block κεφαλίδας
  hp_info->blockCount++;
  BF_Block *header_block;
  BF_Block_Init(&header_block);
  CALL_BF(BF_GetBlock(file_handle, 0, header_block));
  data = BF_Block_GetData(header_block);
  memcpy(data, hp_info, sizeof(HeapFileHeader)); // Αντιγράφουμε το νέο blockCount στο block κεφαλίδας (στη μνήμη που βρίσκεται αυτό δηλαδή)
  BF_Block_SetDirty(header_block);
  CALL_BF(BF_UnpinBlock(header_block));
  BF_Block_Destroy(&header_block);

  // Αρχικοποίηση καινούργιου block (αρχικοποίηση μεταδεδομένων)
  data = BF_Block_GetData(block);
  BlockHeader *header = (BlockHeader*)(data + (BF_BLOCK_SIZE - sizeof(BlockHeader))); // Πάμε στο τέλος που βρίσκεται η κεφαλίδα των block
  header->recordsCount = 0;

  return 1;
}

int HeapFile_InsertRecord(int file_handle, HeapFileHeader *hp_info, const Record record)
{
  BF_Block *block;
  BF_Block_Init(&block);
  char *data;
  BlockHeader *header;
  int maxRecords = (BF_BLOCK_SIZE - sizeof(BlockHeader)) / sizeof(Record); // Μεγιστός αριθμός εγγραφών σε κάθε block (σταθερο το μεγεθος εγγραφών)

  // Αρχικά καλύβουμε όλες τις περιπτώσεις όπου πρεπει να δημιουργηθεί νέο block (block γεμάτο ή υπάρχει μόνο η κεφαλίδα του heap file)
  if (hp_info->blockCount > 1){

    // Παίρνουμε το τελευταίο block
    CALL_BF(BF_GetBlock(file_handle, hp_info->blockCount - 1, block));
    data = BF_Block_GetData(block);

    // Παίρνουμε το ΒlockHeader
    header = (BlockHeader*)(data + (BF_BLOCK_SIZE - sizeof(BlockHeader)));
    if (header->recordsCount == maxRecords){ // Δεν χωράει άλλο record
      CALL_BF(BF_UnpinBlock(block)); // Δεν το χρειαζόμαστε άλλο το γεμάτο τελευταίο block
      if (!createNewBlock(file_handle, hp_info, block)){ // Αν αποτύχει
        BF_Block_Destroy(&block);
        return 0;
      }
      // Παίρνουμε την κεφαλίδα του νέου block
      data = BF_Block_GetData(block);
      header = (BlockHeader*)(data + (BF_BLOCK_SIZE - sizeof(BlockHeader)));
    }

  }else{ // Άμα έχουμε μόνο το block της κεφαλίδας τοτε δημιουργούμε αμέσως καινουργιο block

    if (!createNewBlock(file_handle, hp_info, block)){
      BF_Block_Destroy(&block);
      return 0;
    }
    data = BF_Block_GetData(block);
    header = (BlockHeader*)(data + (BF_BLOCK_SIZE - sizeof(BlockHeader)));
  }

  // Απο εδώ και πέρα το block μας είναι αυτο που θα έχει σίγουρα χώρο για καινούργια εγγραφή (είτε υπήρχε ήδη ή όχι)
  char *recordPtr =  data + (header->recordsCount * sizeof(Record)); // Πάμε στην πρώτη άδεια θέση που υπάρχει στο block
  memcpy(recordPtr, &record, sizeof(Record)); // Αντιγράφουμε σ'αυτήν την θέση την ζητούμενη εγγραφή.
  header->recordsCount++;

  // Set dirty και unpin
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  return 1;
}


HeapFileIterator HeapFile_CreateIterator(int file_handle, HeapFileHeader* header_info, int id)
{
  HeapFileIterator out;
  out.file_handle = file_handle;
  out.searchID = id;
  out.currentRecord = 0;
  BF_Block_Init(&out.block);
  if (header_info->blockCount == 1){
    out.currentBlock = 0;
    BF_Block_Destroy(&out.block);
    out.block = NULL;
    return out;
  }

  BF_GetBlock(file_handle, 1, out.block);
  out.currentBlock = 1;
  return out;
}


int HeapFile_GetNextRecord(HeapFileIterator* heap_iterator, Record** record)
{
  *record=NULL; // Θεωρούμε αρχικά πως δεν έχει βρεθεί κάποια εγγραφή,ώστε να επιστραφεί κάτι (NULL) αμα δεν βρούμε εγγραφή
  if (!heap_iterator || heap_iterator->currentBlock == 0){ // Δεν γίνεται κάποια αναζήτηση έτσι
    return 0;
  }

  // Παίρνουμε το BlockHeader του εκάστοτε block που είναι ο iterator για να πάρουμε τον αριθμό τον records του εκάστοτε block
  char *data = BF_Block_GetData(heap_iterator->block);
  BlockHeader *header = (BlockHeader*)(data + (BF_BLOCK_SIZE - sizeof(BlockHeader)));
  int recordsCount = header->recordsCount;
  
  while (1){
    // Βεβαιωνόμαστε πως ο δείκτης στο τρέχον record δεν έχει ξεπεράσει το όριο του block. Άμα το ξεπερνάει πάμε στο επόμενο block
    if (heap_iterator->currentRecord >= recordsCount){
      int blockCount;
      CALL_BF(BF_GetBlockCounter(heap_iterator->file_handle, &blockCount));

      // Άμα δεν είμαστε στο τελευταίο block τότε πάμε στο επόμενο block
      if (heap_iterator->currentBlock < blockCount - 1){ // Δεν μετράμε την κεφαλίδα
        CALL_BF(BF_UnpinBlock(heap_iterator->block)); // Δεν το χρειαζόμαστε πια το παλιό block

        heap_iterator->currentBlock++;
        heap_iterator->currentRecord = 0; // Απο την αρχή

        CALL_BF(BF_GetBlock(heap_iterator->file_handle, heap_iterator->currentBlock, heap_iterator->block));
        data = BF_Block_GetData(heap_iterator->block); // Ανανεώνουμε τα data
        header = (BlockHeader*)(data + (BF_BLOCK_SIZE - sizeof(BlockHeader)));
        recordsCount = header->recordsCount; // Ανανεώνουμε το recordsCount
        if (recordsCount == 0){ // Άδειο block
          CALL_BF(BF_UnpinBlock(heap_iterator->block));
          BF_Block_Destroy(&heap_iterator->block); // Τελείωσε ο iterator
          return 0;
        }

      }else{ // Είμαστε στο τελευταίο block οπότε δεν υπάρχει αλλή αναζήτηση να γίνει
        CALL_BF(BF_UnpinBlock(heap_iterator->block)); 
        BF_Block_Destroy(&heap_iterator->block);
        return 0;

      }
    }

    Record *rec = (Record*)(data + (heap_iterator->currentRecord++ * sizeof(Record))); // Παίρνουμε το κάθε record του block
    if (rec->id == heap_iterator->searchID){ // Βρήκαμε την αμέσως επόμενη εγγραφή με το ζητούμενο ID
      *record = malloc(sizeof(Record)); // Δεσμεύουμε ένα Record για να το επιστρέψουμε αυτο το record (rec)
      memcpy(*record, rec, sizeof(Record));
      return 1;
    }
  }
  return 0;
}

