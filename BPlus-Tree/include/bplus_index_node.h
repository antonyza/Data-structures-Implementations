#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/

/**
 * @brief Updates the root ID on block 0 (Β-Τree metadata) as well as on the meta pointer.
 * @param fd File descriptor of the file to update the root.
 * @param meta Pointer to the B-Tree Metadata.
 * @param newID New root ID to store.
 * @return 0 on success, -1 on failure.
 */
int updateRootID(int fd, BPlusMeta* meta, int newID);

/**
 * @brief Βρίσκει τον πατέρα του childID με την βοήθεια ενός key που βρίσκεται στον childID. Δουλεύει και για nodes που είναι είτε φύλλα είτε index.
 * @param fd File descriptor of the file to search.
 * @param meta Pointer to the B-Tree Metadata.
 * @param childID Child of the parent we are searching for.
 * @param key Key to search for.
 * @param parentID Parent ID to store.
 * @return 0 on success, -1 on failure.
 */
int findParentID(int fd, BPlusMeta* meta, int childID, int key, int* parentID);


/**
 * @brief Φτιάχνει μία καινούργια ρίζα τύπου index με τα δοσμένα ορίσματα μέσα. Για την δημιουργία μίας νέας ρίζας χρειαζόμαστε πάντα 2 δείκτες και 1 κλειδί.
 * @param fd File descriptor of the file.
 * @param meta Pointer to the B-Tree Metadata.
 * @param leftPointerID Ο αριστερός δείκτης.
 * @param midKey Το μεσαίο κλειδί που έχει έρθει απο κάτω.
 * @param rightPointerID Ο δεξίς δείκτης.
 */
int createRootIndexBlock(int fd, BPlusMeta* meta, int leftPointerID, int midKey, int rightPointerID);

/**
 * @brief Δοκιμάζει αν χωράει ενα νέο κλειδί που έρχεται σε έναν index node. Αν όχι τον σπάει, βάζει το κλειδί στο νέο κόμβο και συνεχίζει πάνω με την ίδια διαδικασία αναδρομικά.
 * @param fd File descriptor of the file to insert data into index node.
 * @param meta Pointer to the B-Tree Metadata.
 * @param indexID ID of the index node to try inserting or splitting.
 * @param midKey Key to insert into this index node.
 * @param rightID Right pointer ID of key.
 * @return 0 on success, -1 on failure.
 */
int recursivelyInsertAndSplit(int fd, BPlusMeta* meta, int indexID, int midKey, int rightID);
#endif