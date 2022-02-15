#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }


    RC IndexManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {

        return PagedFileManager::instance().openFile(fileName, ixFileHandle.fileHandle);
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return PagedFileManager::instance().closeFile(ixFileHandle.fileHandle);
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        // get key size
        short keySize=0;
        if(attribute.type==TypeReal or attribute.type==TypeInt)
        {
            keySize+=4;
        }
        if(attribute.type==TypeVarChar)
        {
            int varCharLength=0;
            memcpy(&varCharLength, (char *) key , 4);
            keySize += 4;
            keySize += varCharLength;
        }

        int keyIValue=0;
        float keyFValue=0.0;
        string keySValue;
        if(attribute.type==TypeInt)
        {
            memcpy(&keyIValue,(char *)key,4);
        }else if (attribute.type==TypeReal)
        {
            memcpy(&keyFValue,(char *)key,4);
        }
        else
        {
            keySValue=string((char *)key+4,keySize-4);
        }

        int totalPageNums=ixFileHandle.fileHandle.getNumberOfPages();
        if(totalPageNums==0) // index file is empty, add the root Page and leaf page
        {
            appendRootPage(ixFileHandle);
            appendLeafPage(ixFileHandle,totalPageNums+1);
            resetRootPointer(ixFileHandle,totalPageNums+1);
            insertEntryToLeafPage(ixFileHandle,totalPageNums+1,-1,attribute,key,keySize,rid);
        }
        else if(totalPageNums==2)
        {
            // read rootPage
            void *readRootPage = malloc(PAGE_SIZE);
            ixFileHandle.fileHandle.readPage(0 , readRootPage);
            char *curP = (char *)readRootPage;
            int rootPage;
            memcpy(&rootPage,curP,4);
            ixFileHandle.fileHandle.writePage(0, readRootPage);       // write page to disk
            free(readRootPage);
            insertEntryToLeafPage(ixFileHandle,rootPage,-1,attribute,key,keySize,rid);
        }
        else
        {
            void *readRootPage = malloc(PAGE_SIZE);
            ixFileHandle.fileHandle.readPage(0 , readRootPage);
            char *curP = (char *)readRootPage;
            int rootPage;
            memcpy(&rootPage,curP,4);
            ixFileHandle.fileHandle.writePage(0, readRootPage);       // write page to disk
            free(readRootPage);
            BtreeSearchArray.clear();

            short leafNodeChecker=0;
            int nextPageNum=rootPage;

            while(leafNodeChecker!=1) { // loop until reach the leaf page
                void *readPage = malloc(PAGE_SIZE);
                ixFileHandle.fileHandle.readPage(nextPageNum, readPage);
                curP = (char *) readPage;
                memcpy(&leafNodeChecker, curP + PAGE_SIZE - 6, 2);
                if(leafNodeChecker!=1){BtreeSearchArray.push_back(nextPageNum);}
                else{break;}
                short freeBytes;
                short slotNum;
                memcpy(&freeBytes,curP+PAGE_SIZE-2,2);
                memcpy(&slotNum,curP+PAGE_SIZE-4,2);
                short dataSize = keySize + 4;
                short insertIndex;
                int tempPage=0;
                if (attribute.type == TypeVarChar) {
                    bool find = false;
                    string compareLast = string(curP + 4 + (slotNum - 1) * dataSize + 4, keySize - 4);
                    if (keySValue > compareLast) {
                        insertIndex = slotNum;
                        find = true;
                    }
                    short left = 0;
                    short right = slotNum - 1;
                    if (find == false) {
                        while (left < right) {
                            short mid = left + (right - left) / 2;
                            string compareMid = string(curP + 4 + mid * dataSize + 4, keySize - 4);
                            if (keySValue > compareMid) {
                                left = mid + 1;
                            } else {
                                right = mid;
                            }
                        }
                        insertIndex = left;
                    }
                } else if (attribute.type == TypeInt) {
                    bool find = false;
                    int compareLast;
                    memcpy(&compareLast, curP + 4 + (slotNum - 1) * dataSize, 4);
                    if (keyIValue > compareLast) {
                        insertIndex = slotNum;
                        find = true;
                    }
                    short left = 0;
                    short right = slotNum - 1;
                    if (find == false) {
                        while (left < right) {
                            short mid = left + (right - left) / 2;
                            int compareMid;
                            memcpy(&compareMid, curP + 4 + mid * dataSize, 4);
                            if (keyIValue > compareMid) {
                                left = mid + 1;
                            } else {
                                right = mid;
                            }
                        }
                        insertIndex = left;
                    }

                } else {
                    bool find = false;
                    float compareLast;
                    memcpy(&compareLast, curP + 4 + (slotNum - 1) * dataSize, 4);
                    if (keyFValue > compareLast) {
                        insertIndex = slotNum;
                        find = true;
                    }
                    short left = 0;
                    short right = slotNum - 1;
                    if (find == false) {
                        while (left < right) {
                            short mid = left + (right - left) / 2;
                            int compareMid;
                            memcpy(&compareMid, curP + 4 + mid * dataSize, 4);
                            if (keyFValue > compareMid) {
                                left = mid + 1;
                            } else {
                                right = mid;
                            }
                        }
                        insertIndex = left;
                    }
                }
                if (insertIndex == 0) {
                    memcpy(&tempPage, curP, 4);
                } else {
                    memcpy(&tempPage, curP + insertIndex * dataSize, 4);
                }
                ixFileHandle.fileHandle.writePage(nextPageNum, readPage);       // write page to disk
                free(readPage);
                nextPageNum=tempPage;
            }
            //reach the leaf page, leaf page num in nextPage
            searchIndex=BtreeSearchArray.size()-1;
            int interiorPageNum=BtreeSearchArray[searchIndex];
            insertEntryToLeafPage(ixFileHandle,nextPageNum,interiorPageNum,attribute,key,keySize,rid);

        }

        return 0;
    }

    RC IndexManager::appendRootPage(IXFileHandle &ixFileHandle)
    {
        void * appendRootPage= malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(appendRootPage);
        free(appendRootPage);
        ixFileHandle.fileHandle.initializePage(0);
        return 0;
    }

    RC IndexManager::appendInteriorPage(IXFileHandle &ixFileHandle, PageNum pageNum)
    {
        void * appendInteriorPage= malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(appendInteriorPage);
        free(appendInteriorPage);
        ixFileHandle.fileHandle.initializeIndexInteriorPage(pageNum);
        return 0;
    }

    RC IndexManager::insertEntryToInteriorPage(IXFileHandle &ixFileHandle, PageNum pageNum,PageNum InteriorPageNum, const Attribute &attribute, const void *key, short keySize,PageNum Lpointer,PageNum Rpointer)
    {
        void *readPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(pageNum , readPage);
        char *curP = (char *)readPage;
        short freeBytes;
        short slotNum;
        memcpy(&freeBytes,curP+PAGE_SIZE-2,2);
        memcpy(&slotNum,curP+PAGE_SIZE-4,2);
        short newFreeBytes=0;
        short dataSize=keySize+4; // 4-- right pointer
        int keyIValue=0;
        float keyFValue=0.0;
        string keySValue;
        if(attribute.type==TypeInt)
        {
            memcpy(&keyIValue,(char *)key,4);
        }else if (attribute.type==TypeReal)
        {
            memcpy(&keyFValue,(char *)key,4);
        }else
        {
            keySValue=string((char *)key+4,keySize-4);
        }

        short insertIndex;
        if(slotNum==0){insertIndex=0;}
        if(attribute.type==TypeVarChar) {
            bool find=false;
            string compareLast = string(curP +4+ (slotNum - 1) * dataSize + 4, keySize - 4);
            if(keySValue>compareLast){insertIndex=slotNum;find=true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while(left<right)
                {
                    short mid=left+(right-left)/2;
                    string compareMid = string(curP +4+ mid * dataSize + 4, keySize - 4);
                    if(keySValue>compareMid){
                        left=mid+1;
                    }else
                    {
                        right=mid;
                    }
                }
                insertIndex=left;
            }
        }else if (attribute.type==TypeInt)
        {
            bool find=false;
            int compareLast;
            memcpy(&compareLast,curP +4+ (slotNum - 1) * dataSize,4);
            if(keyIValue>compareLast){insertIndex=slotNum;find=true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while(left<right)
                {
                    short mid=left+(right-left)/2;
                    int compareMid;
                    memcpy(&compareMid,curP +4+ mid * dataSize,4);
                    if(keyIValue>compareMid){
                        left=mid+1;
                    }else
                    {
                        right=mid;
                    }
                }
                insertIndex=left;
            }

        }else
        {
            bool find=false;
            float compareLast;
            memcpy(&compareLast,curP + 4+(slotNum - 1) * dataSize,4);
            if(keyFValue>compareLast){insertIndex=slotNum;find=true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while(left<right)
                {
                    short mid=left+(right-left)/2;
                    int compareMid;
                    memcpy(&compareMid,curP + 4 + mid * dataSize,4);
                    if(keyFValue>compareMid){
                        left=mid+1;
                    }else
                    {
                        right=mid;
                    }
                }
                insertIndex=left;
            }
        }

        if ((freeBytes - dataSize)>= 0) // not split
        {
            short migrateNums=slotNum-insertIndex;    // migrate other keys backward
            memcpy(curP+4+(insertIndex+1)*dataSize,curP+4+insertIndex*dataSize,migrateNums*dataSize);
            memcpy(curP+4+insertIndex*dataSize-4,&Lpointer,4);
            memcpy(curP+4+insertIndex*dataSize,key,keySize);
            memcpy(curP+4+insertIndex*dataSize+keySize,&Rpointer,4);

            slotNum += 1;
            newFreeBytes = freeBytes - dataSize;
            memcpy(curP + PAGE_SIZE - 2, &newFreeBytes, 2);
            memcpy(curP + PAGE_SIZE - 4, &slotNum, 2);
        }
        else //split the interior Page
        {
            void * halfPage= malloc(PAGE_SIZE);
            void * interiorKey=malloc(keySize);
            char * Phalf=(char *)halfPage;
            char * PinteriorKey=(char *)interiorKey;
            short dataNums=0;
            int splitFlag=0;  // 0 split left   , 1 split right
            if(insertIndex>=(slotNum+1)/2) // insert num in halfpage and do a right spilt
            {
                memcpy(Phalf,curP+((slotNum+1)/2)*dataSize,4+(slotNum/2)*dataSize);
                memset(curP+4+((slotNum+1)/2)*dataSize,0,(slotNum/2)*dataSize);
                short newInsertIndex=insertIndex-(slotNum+1)/2;
                short newMigrateNums=(slotNum/2) -newInsertIndex;
                memcpy(Phalf+4+(insertIndex+1)*dataSize,Phalf+4+insertIndex*dataSize,newMigrateNums*dataSize);
                memcpy(Phalf+4+insertIndex*dataSize-4,&Lpointer,4);
                memcpy(Phalf+4+insertIndex*dataSize,key,keySize);
                memcpy(Phalf+4+insertIndex*dataSize+keySize,&Rpointer,4);
                short nSlotNum=(slotNum+1)/2;
                short nfreeBytes=freeBytes+(slotNum/2)*dataSize;
                memcpy(curP + PAGE_SIZE - 2, &nfreeBytes, 2);
                memcpy(curP + PAGE_SIZE - 4, &nSlotNum, 2);
                dataNums=slotNum/2;
                memcpy(PinteriorKey,Phalf+4,keySize);
                memcpy(Phalf,Phalf+4+keySize,4+(slotNum/2)*dataSize);
                memset(Phalf+4+(slotNum/2)*dataSize,0,4+keySize);
                splitFlag=1;// 0 split left   , 1 split right
            }else  // do a left split
            {
                memcpy(Phalf,curP,4+((slotNum-1)/2)*dataSize);
                memset(curP,0,((slotNum-1)/2)*dataSize);
                memcpy(curP,curP+((slotNum-1)/2)*dataSize,4+((slotNum/2)+1)*dataSize );
                memset(curP+4+((slotNum/2)+1)*dataSize,0,((slotNum-1)/2)*dataSize);
                short newInsertIndex=insertIndex;
                short newMigrateNums=((slotNum-1)/2) -newInsertIndex;
                memcpy(Phalf+4+(insertIndex+1)*dataSize,Phalf+4+insertIndex*dataSize,newMigrateNums*dataSize);
                memcpy(Phalf+4+insertIndex*dataSize-4,&Lpointer,4);
                memcpy(Phalf+4+insertIndex*dataSize,key,keySize);
                memcpy(Phalf+4+insertIndex*dataSize+keySize,&Rpointer,4);
                short nSlotNum=slotNum/2;
                short nfreeBytes=freeBytes+((slotNum-1)/2)*dataSize+dataSize;
                memcpy(curP + PAGE_SIZE - 2, &nfreeBytes, 2);
                memcpy(curP + PAGE_SIZE - 4, &nSlotNum, 2);
                dataNums=((slotNum-1)/2)+1;
                memcpy(PinteriorKey,curP+4,keySize);
                memcpy(curP,curP+4+keySize,4+(slotNum/2)*dataSize);
                memset(curP+4+(slotNum/2)*dataSize,0,4+keySize);
                splitFlag=0;// 0 split left   , 1 split right
            }

            int totalNums=ixFileHandle.fileHandle.getNumberOfPages();
            int splitPageNum=totalNums;
            int originalPageNum=pageNum;


            //set append page pointer
            if(splitFlag==0) { // 0 split left   , 1 split right
                splitInteriorPage(ixFileHandle, originalPageNum,splitPageNum, attribute, interiorKey, halfPage, dataNums, keySize,
                              InteriorPageNum, splitPageNum, originalPageNum, splitFlag);
            }
            else
            {
                splitInteriorPage(ixFileHandle, originalPageNum,splitPageNum, attribute, interiorKey, halfPage, dataNums, keySize,
                              InteriorPageNum, originalPageNum, splitPageNum, splitFlag);
            }
            free(halfPage);
            free(interiorKey);
        }
        ixFileHandle.fileHandle.writePage(pageNum, readPage);       // write page to disk

        free(readPage);
        return 0;
    }

    RC IndexManager::appendLeafPage(IXFileHandle &ixFileHandle, PageNum pageNum)
    {
        void * appendLeafPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(appendLeafPage);
        free(appendLeafPage);
        ixFileHandle.fileHandle.initializeIndexLeafNodePage(pageNum);
        return 0;
    }

    RC IndexManager::insertEntryToLeafPage(IXFileHandle &ixFileHandle, PageNum pageNum,PageNum InteriorPageNum,const Attribute &attribute, const void *key, short keySize,const RID &rid)
    {
        void *readPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(pageNum , readPage);
        char *curP = (char *)readPage;
        short freeBytes;
        short slotNum;
        int keyIValue=0;
        float keyFValue=0.0;
        string keySValue;
        if(attribute.type==TypeInt)
        {
            memcpy(&keyIValue,(char *)key,4);
        }else if (attribute.type==TypeReal)
        {
            memcpy(&keyFValue,(char *)key,4);
        }else
        {
            keySValue=string((char *)key+4,keySize-4);
        }

        memcpy(&freeBytes,curP+PAGE_SIZE-2,2);
        memcpy(&slotNum,curP+PAGE_SIZE-4,2);
        short dataSize=keySize+ sizeof(RID);
        short newFreeBytes=freeBytes-dataSize;

        // find a place to insert using binary search
        short insertIndex;
        if(slotNum==0){insertIndex=0;}
        if(attribute.type==TypeVarChar) {
            bool find=false;
            string compareLast = string(curP + (slotNum - 1) * dataSize + 4, keySize - 4);
            if(keySValue>compareLast){insertIndex=slotNum;find= true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while(left<right)
                {
                    short mid=left+(right-left)/2;
                    string compareMid = string(curP + mid * dataSize + 4, keySize - 4);
                    if(keySValue>compareMid){
                        left=mid+1;
                    }else
                    {
                        right=mid;
                    }
                }
                insertIndex=left;
            }

        }else if (attribute.type==TypeInt)
        {
            bool find=false;
            int compareLast;
            memcpy(&compareLast,curP + (slotNum - 1) * dataSize,4);
            if(keyIValue>compareLast){insertIndex=slotNum;find= true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while(left<right)
                {
                    short mid=left+(right-left)/2;
                    int compareMid;
                    memcpy(&compareMid,curP + mid * dataSize,4);
                    if(keyIValue>compareMid){
                        left=mid+1;
                    }else
                    {
                        right=mid;
                    }
                }
                insertIndex=left;
            }

        }else
        {
            bool find=false;
            float compareLast;
            memcpy(&compareLast,curP + (slotNum - 1) * dataSize,4);
            if(keyFValue>compareLast){insertIndex=slotNum;find= true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while( (left<right) )
                {
                    short mid=left+(right-left)/2;
                    int compareMid;
                    memcpy(&compareMid,curP + mid * dataSize,4);
                    if(keyFValue>compareMid){
                        left=mid+1;
                    }else
                    {
                        right=mid;
                    }
                }
                insertIndex=left;
            }
        }

        if(newFreeBytes>=0) {               // do not need to split
            // before insert, we need to migrate other entry
            short migrateNums=slotNum-insertIndex;
            memcpy(curP+(insertIndex+1)*dataSize,curP+insertIndex*dataSize,migrateNums*dataSize);

            memcpy(curP + insertIndex * dataSize, key, keySize);
            memcpy(curP + insertIndex * dataSize + keySize , &rid, sizeof(RID));
            slotNum += 1;
            memcpy(curP + PAGE_SIZE - 2, &newFreeBytes, 2);
            memcpy(curP + PAGE_SIZE - 4, &slotNum, 2);
        }
        else  // need to split, append a new interior page and a new leaf page
        {
            void * halfPage= malloc(PAGE_SIZE);
            void * interiorKey=malloc(keySize);
            char * Phalf=(char *)halfPage;
            char * PinteriorKey=(char *)interiorKey;
            short dataNums=0;
            int splitFlag=0;  // 0 split left   , 1 split right
            if(insertIndex>=(slotNum+1)/2) // insert num in halfpage and do a right spilt
            {
                memcpy(Phalf,curP+((slotNum+1)/2)*dataSize,(slotNum/2)*dataSize);
                memset(curP+((slotNum+1)/2)*dataSize,0,(slotNum/2)*dataSize);
                short newInsertIndex=insertIndex-(slotNum+1)/2;
                short newMigrateNums=(slotNum/2) -newInsertIndex;
                memcpy(Phalf+(newInsertIndex+1)*dataSize,Phalf+newInsertIndex*dataSize,newMigrateNums*dataSize);
                memcpy(Phalf + newInsertIndex * dataSize, key, keySize);
                memcpy(Phalf + newInsertIndex * dataSize + keySize , &rid, sizeof(RID));
                short nSlotNum=(slotNum+1)/2;
                short nfreeBytes=freeBytes+(slotNum/2)*dataSize;
                memcpy(curP + PAGE_SIZE - 2, &nfreeBytes, 2);
                memcpy(curP + PAGE_SIZE - 4, &nSlotNum, 2);
                dataNums=(slotNum/2)+1;
                memcpy(PinteriorKey,Phalf,keySize);
                splitFlag=1;// 0 split left   , 1 split right
            }else  // do a left split
            {
                memcpy(Phalf,curP,((slotNum-1)/2)*dataSize);
                memset(curP,0,((slotNum-1)/2)*dataSize);
                memcpy(curP,curP+((slotNum-1)/2)*dataSize,((slotNum/2)+1)*dataSize );
                memset(curP+((slotNum/2)+1)*dataSize,0,((slotNum-1)/2)*dataSize);
                short newInsertIndex=insertIndex;
                short newMigrateNums=((slotNum-1)/2) -newInsertIndex;
                memcpy(Phalf+(newInsertIndex+1)*dataSize,Phalf+newInsertIndex*dataSize,newMigrateNums*dataSize);
                memcpy(Phalf + newInsertIndex * dataSize, key, keySize);
                memcpy(Phalf + newInsertIndex * dataSize + keySize , &rid, sizeof(RID));
                short nSlotNum=(slotNum/2)+1;
                short nfreeBytes=freeBytes+((slotNum-1)/2)*dataSize;
                memcpy(curP + PAGE_SIZE - 2, &nfreeBytes, 2);
                memcpy(curP + PAGE_SIZE - 4, &nSlotNum, 2);
                dataNums=((slotNum-1)/2)+1;
                memcpy(PinteriorKey,Phalf,keySize);
                splitFlag=0;// 0 split left   , 1 split right
            }

            int totalNums=ixFileHandle.fileHandle.getNumberOfPages();
            int splitPageNum=totalNums;
            int originalPageNum=pageNum;


            //set append page pointer
            if(splitFlag==0) { // 0 split left   , 1 split right
                splitLeafPage(ixFileHandle, originalPageNum,splitPageNum, attribute, interiorKey, halfPage, dataNums, keySize,
                              InteriorPageNum, splitPageNum, originalPageNum, splitFlag);
            }
            else
            {
                memcpy(curP + PAGE_SIZE - 10, &splitPageNum, 4);
                splitLeafPage(ixFileHandle, originalPageNum,splitPageNum, attribute, interiorKey, halfPage, dataNums, keySize,
                              InteriorPageNum, originalPageNum, splitPageNum, splitFlag);
            }
            free(halfPage);
            free(interiorKey);
        }
        ixFileHandle.fileHandle.writePage(pageNum, readPage);       // write page to disk

        free(readPage);
        return 0;
    }

    RC IndexManager::splitLeafPage(IXFileHandle &ixFileHandle,PageNum originalPageNum,PageNum splitPageNum,const Attribute &attribute,const void * interiorKey,const void * halfPageData, short dataNums,short keySize,PageNum interiorPageNum, PageNum leftPointer,PageNum rightPointer,int splitFlag)
    {
        appendLeafPage(ixFileHandle,splitPageNum);
        writeSplitLeafPage(ixFileHandle,originalPageNum,splitPageNum,halfPageData,dataNums,keySize,splitFlag);
        if(interiorPageNum==-1) {// need to append a new parent interior page
            int totalNums=ixFileHandle.fileHandle.getNumberOfPages();
            interiorPageNum=totalNums;
            appendInteriorPage(ixFileHandle, interiorPageNum);
            resetRootPointer(ixFileHandle,interiorPageNum);
        }

        if(BtreeSearchArray.size()<2) {
            insertEntryToInteriorPage(ixFileHandle, interiorPageNum, -1, attribute, interiorKey, keySize,
                                      leftPointer,
                                      rightPointer);
        }
        else
        {
            searchIndex-=1;
            int interiorParentPageNum = BtreeSearchArray[searchIndex];
            insertEntryToInteriorPage(ixFileHandle, interiorPageNum, interiorParentPageNum, attribute, interiorKey, keySize,
                                      leftPointer,
                                      rightPointer);
        }
    }

    RC IndexManager::writeSplitLeafPage(IXFileHandle &ixFileHandle,PageNum originalPageNum, PageNum pageNum, const void *halfPageData,short dataNums,short keySize,int splitFlag)
    {
        void *readPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(pageNum , readPage);
        char *curP = (char *)readPage;
        memcpy(curP, halfPageData,dataNums*(keySize+sizeof(RID)) );
        short newfreeBytes=4086-dataNums*(keySize+sizeof(RID)) ;
        short newSlotNum=dataNums;
        memcpy(curP + PAGE_SIZE - 2, &newfreeBytes, 2);
        memcpy(curP + PAGE_SIZE - 4, &newSlotNum, 2);
        if(splitFlag==0)
        {
            memcpy(curP + PAGE_SIZE - 10, &originalPageNum, 4);
        }
        ixFileHandle.fileHandle.writePage(pageNum, readPage);       // write page to disk
        free(readPage);
    }

    RC IndexManager::splitInteriorPage(IXFileHandle &ixFileHandle, PageNum originalPageNum, PageNum splitPageNum,const Attribute &attribute, const void *interiorKey, const void *halfPageData,short dataNums, short keySize, PageNum interiorPageNum, PageNum leftPointer,PageNum rightPointer, int splitFlag)
    {
        appendInteriorPage(ixFileHandle,splitPageNum);
        writeSplitInteriorPage(ixFileHandle,originalPageNum,splitPageNum,halfPageData,dataNums,keySize,splitFlag);
        if(interiorPageNum==-1) {   // need to append a new parent interior page
            int totalNums=ixFileHandle.fileHandle.getNumberOfPages();
            interiorPageNum=totalNums;
            appendInteriorPage(ixFileHandle, interiorPageNum);
            resetRootPointer(ixFileHandle,interiorPageNum);
        }
        if(searchIndex==0){
            insertEntryToInteriorPage(ixFileHandle,interiorPageNum,-1,attribute,interiorKey,keySize,leftPointer,rightPointer);
        }else
        {
            searchIndex-=1;
            int interiorParentPageNum = BtreeSearchArray[searchIndex];
            insertEntryToInteriorPage(ixFileHandle,interiorPageNum,interiorParentPageNum,attribute,interiorKey,keySize,leftPointer,rightPointer);
        }
    }

    RC IndexManager::writeSplitInteriorPage(IXFileHandle &ixFileHandle, PageNum originalPageNum, PageNum pageNum,const void *halfPageData, short dataNums, short keySize, int splitFlag)
    {
        void *readPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(pageNum , readPage);
        char *curP = (char *)readPage;
        memcpy(curP, halfPageData,4+dataNums*(keySize+4) );
        short newfreeBytes=4090-4-dataNums*(keySize+4) ;
        short newSlotNum=dataNums;
        memcpy(curP + PAGE_SIZE - 2, &newfreeBytes, 2);
        memcpy(curP + PAGE_SIZE - 4, &newSlotNum, 2);
        ixFileHandle.fileHandle.writePage(pageNum, readPage);       // write page to disk
        free(readPage);
    }

    RC IndexManager::resetRootPointer(IXFileHandle &ixFileHandle, PageNum rootPointerPageNum)
    {
        void *readPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(0 , readPage);
        char *curP = (char *)readPage;
        int rootPointer=rootPointerPageNum;   //set pointer
        memcpy(curP,&rootPointer,4);
        ixFileHandle.fileHandle.writePage(0, readPage);       // write page to disk
        free(readPage);
        return 0;
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    IXFileHandle &IXFileHandle::instance() {
        static IXFileHandle ixFileHandle = IXFileHandle();
        return ixFileHandle;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
        fileHandle.closeFile();
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        fileHandle.collectCounterValues(readPageCount,writePageCount,appendPageCount);
        return 0;
    }

} // namespace PeterDB