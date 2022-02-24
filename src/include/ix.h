#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {


    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;
        PagedFileManager* pfm;

        RC appendRootPage(IXFileHandle &ixFileHandle);
        RC insertEntryToInteriorPage(IXFileHandle &ixFileHandle,PageNum pageNum,PageNum InteriorPageNum,const Attribute &attribute,const void *key,short keySize,PageNum Lpointer,PageNum Rpointer);
        RC insertEntryToLeafPage(IXFileHandle &ixFileHandle,PageNum pageNum,PageNum InteriorPageNum,const Attribute &attribute,const void *key,short keySize,const RID &rid);
        RC splitLeafPage(IXFileHandle &ixFileHandle,PageNum originalPageNum, PageNum splitPageNum,const Attribute &attribute,const void * interiorKey,const void * halfPageData,short dataNums,short keySize, PageNum interiorPageNum, PageNum leftPointer,PageNum rightPointer,short stringDataSize);
        RC splitInteriorPage(IXFileHandle &ixFileHandle,PageNum originalPageNum, PageNum splitPageNum,const Attribute &attribute,const void * interiorKey,const void * halfPageData,short dataNums,short keySize, PageNum interiorPageNum, PageNum leftPointer,PageNum rightPointer,short stringDataSize);
        RC resetRootPointer(IXFileHandle &ixFileHandle,PageNum rootPointerPageNum);
        RC appendAndWriteLeafPage(IXFileHandle &ixFileHandle,PageNum originalPageNum,PageNum pageNum,const void * halfPageData,short dataNums,short keySize,short stringDataSize,const Attribute &attribute);
        RC appendAndWriteInteriorPage(IXFileHandle &ixFileHandle,PageNum originalPageNum,PageNum pageNum,const void * halfPageData,short dataNums,short keySize,short stringDataSize,const Attribute &attribute);
        RC resetNextPagePointer(IXFileHandle &ixFileHandle,PageNum );

        RC searchLeftMostEntry(IXFileHandle &ixFileHandle, PageNum &entryPageNum ,short &entryOffSet, short &entryIndex, void *leafPageBuffer);
        RC searchEntry(IXFileHandle &ixFileHandle,const Attribute &attribute, const void *key, PageNum &entryPageNum , void *leafPageBuffer);
        RC printPage(IXFileHandle &ixFileHandle, const Attribute &attribute,PageNum pageNum, std::ostream &out) const;

    private:
        vector<int> BtreeSearchArray;
        int searchIndex;


    protected:
        IndexManager() = default;                                                // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        RC initScanIterator(IXFileHandle &ixFileHandle,
                            const Attribute &attribute,
                            const void *lowKey,
                            const void *highKey,
                            bool lowKeyInclusive,
                            bool highKeyInclusive,IndexManager* ix);
        // Terminate index scan
        RC close();
        IXFileHandle* ixFileHandle;
    private:
        Attribute attribute;
        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        IndexManager* ix;
        PageNum curPageNum;
        short curOffSet;
        short curIndex;
        void *curPageBuffer;
        short curSlotNum;
    };
    class IXFileHandle {
    public:
        static IXFileHandle &instance();
        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        FileHandle fileHandle;
    };


}// namespace PeterDB
#endif // _ix_h_
