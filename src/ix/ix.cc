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
        float keyFValue;
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

            void* data= malloc(PAGE_SIZE);
            char * Pdata=(char *)data;
            memcpy(Pdata,key,keySize);
            memcpy(Pdata+keySize,&rid.pageNum,sizeof(unsigned ));
            memcpy(Pdata+keySize+sizeof(unsigned ),&rid.slotNum,sizeof(short));

            appendAndWriteLeafPage(ixFileHandle,-1,totalPageNums+1,data,1,keySize);
            free(data);
            return 0;
        }
        else if(totalPageNums==2)
        {
            // read rootPage
            void *readRootPage = malloc(PAGE_SIZE);
            ixFileHandle.fileHandle.readPage(0 , readRootPage);
            char *curP = (char *)readRootPage;
            int rootPage;
            memcpy(&rootPage,curP,4);
//            ixFileHandle.fileHandle.writePage(0, readRootPage);       // write page to disk
            free(readRootPage);
            insertEntryToLeafPage(ixFileHandle,rootPage,-1,attribute,key,keySize,rid);
            return 0;
        }
        else{ //totalnum>2
            void *readRootPage = malloc(PAGE_SIZE);
            ixFileHandle.fileHandle.readPage(0 , readRootPage);
            char *curP = (char *)readRootPage;
            int rootPage;
            memcpy(&rootPage,curP,4);
//            ixFileHandle.fileHandle.writePage(0, readRootPage);       // write page to disk
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
                else{
                    break;}
                short freeBytes;
                short slotNum;
                memcpy(&freeBytes,curP+PAGE_SIZE-2,2);
                memcpy(&slotNum,curP+PAGE_SIZE-4,2);
                short dataSize = keySize + 4;
                short insertIndex;


                if (attribute.type == TypeVarChar) {
                    bool find = false;
                    string compareLast = string(curP + 4 + (slotNum - 1) * dataSize + 4, keySize - 4);
                    string compareFirst= string(curP + 4  + 4, keySize - 4);
                    if(keySValue<compareFirst){
                        insertIndex = 0;
                        find = true;
                    }
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
                            if(keySValue == compareMid)
                            {
                                left=mid+1;
                                break;
                            }
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
                    int compareFirst;
                    memcpy(&compareFirst, curP + 4 , 4);
                    if (keyIValue < compareFirst) {
                        insertIndex = 0;
                        find = true;
                    }
                    if (keyIValue >= compareLast) {
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
                            if(keyIValue == compareMid)
                            {
                                left=mid+1;
                                break;
                            }
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
                    float compareFirst;
                    memcpy(&compareFirst, curP + 4 , 4);
                    if (keyFValue < compareFirst) {
                        insertIndex = 0;
                        find = true;
                    }
                    if (keyFValue >= compareLast) {
                        insertIndex = slotNum;
                        find = true;
                    }
                    short left = 0;
                    short right = slotNum - 1;
                    if (find == false) {
                        while (left < right) {
                            short mid = left + (right - left) / 2;
                            float compareMid;
                            memcpy(&compareMid, curP + 4 + mid * dataSize, 4);
                            if(keyFValue == compareMid)
                            {
                                left=mid+1;
                                break;
                            }
                            if (keyFValue > compareMid) {
                                left = mid + 1;
                            } else {
                                right = mid;
                            }
                        }
                        insertIndex = left;
                    }
                }

                memcpy(&nextPageNum, curP + insertIndex * dataSize, 4);

                free(readPage);
            }
            //reach the leaf page, leaf page num in nextPage
            searchIndex=BtreeSearchArray.size()-1;
            int interiorPageNum=BtreeSearchArray[searchIndex];
            insertEntryToLeafPage(ixFileHandle,nextPageNum,interiorPageNum,attribute,key,keySize,rid);
            BtreeSearchArray.clear();
            searchIndex=0;
            return 0;
        }

        return -1;
    }

    RC IndexManager::appendRootPage(IXFileHandle &ixFileHandle)
    {
        void * appendRootPage= malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(appendRootPage);
        free(appendRootPage);
        ixFileHandle.fileHandle.initializeRootPage(0);
        return 0;
    }

    RC IndexManager::appendAndWriteLeafPage(IXFileHandle &ixFileHandle, PageNum originalPageNum, PageNum pageNum,const void *halfPageData, short dataNums, short keySize)
    {
        void * appendLeafPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(appendLeafPage);
        free(appendLeafPage);
        void * newIndexLeafNodePage= malloc(PAGE_SIZE);

        short leafNodeChecker=1; // 0 means it is an unleafNode page
        int nextLeafPagePageNum=-1; // -1 means currently no other leaf page PageNum
        char * cur=(char *)newIndexLeafNodePage;

        memcpy(cur+PAGE_SIZE-6,&leafNodeChecker,2);
        memcpy(cur+PAGE_SIZE-10,&nextLeafPagePageNum,4);

        int curNext=-1;

        memcpy(&curNext, (char *) halfPageData + PAGE_SIZE - 10, 4);

        if(curNext>0)
        {
            memcpy(cur+PAGE_SIZE-10,&curNext,4);
        }

        memcpy(cur, (char *)halfPageData,dataNums*(keySize+sizeof(unsigned )+sizeof(short) ) );
        short newfreeBytes=4086-dataNums*(keySize+sizeof(unsigned )+sizeof(short)) ;
        short newSlotNum=dataNums;
        memcpy(cur + PAGE_SIZE - 2, &newfreeBytes, 2);
        memcpy(cur + PAGE_SIZE - 4, &newSlotNum, 2);

        ixFileHandle.fileHandle.writePage(pageNum,newIndexLeafNodePage);
        free(newIndexLeafNodePage);
        return 0;
    }

    RC IndexManager::appendAndWriteInteriorPage(IXFileHandle &ixFileHandle, PageNum originalPageNum, PageNum pageNum,const void *halfPageData, short dataNums, short keySize)
    {
        void * appendInteriorPage= malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.appendPage(appendInteriorPage);
        free(appendInteriorPage);
        void * newIndexInteriorPage= malloc(PAGE_SIZE);
        short slotNum=0;
        short freeBytes=4090;
        short leafNodeChecker=0; // 0 means it is an unleafNode page

        char * cur=(char *)newIndexInteriorPage;

        memcpy(cur+PAGE_SIZE-6,&leafNodeChecker,2);

        memcpy(cur, (char *)halfPageData,4+dataNums*(keySize+4) );
        short newfreeBytes=4090-4-dataNums*(keySize+4) ;
        short newSlotNum=dataNums;
        memcpy(cur + PAGE_SIZE - 2, &newfreeBytes, 2);
        memcpy(cur + PAGE_SIZE - 4, &newSlotNum, 2);
        if(pageNum==0){return -1;}
        ixFileHandle.fileHandle.writePage(pageNum,newIndexInteriorPage);
        free(newIndexInteriorPage);
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
        float keyFValue;
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
        short dataSize=keySize+ sizeof(unsigned )+sizeof(short);


        // find a place to insert using binary search
        short insertIndex;
        if(slotNum==0){insertIndex=0;}
        if(attribute.type==TypeVarChar) {
            bool find=false;
            string compareLast = string(curP + (slotNum - 1) * dataSize + 4, keySize - 4);
            string compareFirst= string(curP +  4, keySize - 4);
            if(keySValue<compareFirst){
                insertIndex = 0;
                find = true;
            }
            if(keySValue>=compareLast){insertIndex=slotNum;find= true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while(left<right)
                {
                    short mid=left+(right-left)/2;
                    string compareMid = string(curP + mid * dataSize + 4, keySize - 4);
                    if(keySValue == compareMid)
                    {
                        left=mid+1;
                        break;
                    }
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
            int compareFirst;
            memcpy(&compareFirst, curP , 4);
            if (keyIValue < compareFirst) {
                insertIndex = 0;
                find = true;
            }
            if(keyIValue>=compareLast){insertIndex=slotNum;find= true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while(left<right)
                {
                    short mid=left+(right-left)/2;
                    int compareMid;
                    memcpy(&compareMid,curP + mid * dataSize,4);
                    if(keyIValue == compareMid)
                    {
                        left=mid+1;
                        break;
                    }
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
            float compareFirst;
            memcpy(&compareFirst, curP , 4);
            if (keyFValue < compareFirst) {
                insertIndex = 0;
                find = true;
            }
            if(keyFValue>=compareLast){insertIndex=slotNum;find= true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while( (left<right) )
                {
                    short mid=left+(right-left)/2;
                    float compareMid;
                    memcpy(&compareMid,curP + mid * dataSize,4);
                    if(keyFValue == compareMid)
                    {
                        left=mid+1;
                        break;
                    }
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

        if((freeBytes-dataSize)>=0) {               // do not need to split
            // before insert, we need to migrate other entry
            short migrateNums=slotNum-insertIndex;
            memcpy(curP+(insertIndex+1)*dataSize,curP+insertIndex*dataSize,migrateNums*dataSize);

            memcpy(curP + insertIndex * dataSize, key, keySize);
            memcpy(curP + insertIndex * dataSize + keySize , &rid.pageNum, sizeof(unsigned));
            memcpy(curP + insertIndex * dataSize + keySize+sizeof(unsigned) , &rid.slotNum, sizeof(short));
            slotNum += 1;
            short newFreeBytes=freeBytes-dataSize;
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
           // 0 split insert entry in left   , 1 split right
            if(insertIndex>=(slotNum+1)/2) // insert num in halfpage and do a right spilt
            {
                memcpy(Phalf,curP+((slotNum+1)/2)*dataSize,(slotNum/2)*dataSize);
                memset(curP+((slotNum+1)/2)*dataSize,0,(slotNum/2)*dataSize);
                short newInsertIndex=insertIndex-(slotNum+1)/2;
                short newMigrateNums=(slotNum/2) -newInsertIndex;
                memcpy(Phalf+(newInsertIndex+1)*dataSize,Phalf+newInsertIndex*dataSize,newMigrateNums*dataSize);
                memcpy(Phalf + newInsertIndex * dataSize, key, keySize);
                memcpy(Phalf + newInsertIndex * dataSize + keySize , &rid.pageNum, sizeof(unsigned));
                memcpy(Phalf + newInsertIndex * dataSize + keySize+sizeof(unsigned) , &rid.slotNum, sizeof(short));
                short nSlotNum=(slotNum+1)/2;
                short nfreeBytes=freeBytes+(slotNum/2)*dataSize;
                memcpy(curP + PAGE_SIZE - 2, &nfreeBytes, 2);
                memcpy(curP + PAGE_SIZE - 4, &nSlotNum, 2);
                dataNums=(slotNum/2)+1;
                memcpy(PinteriorKey,Phalf,keySize);
                int currentPointer;
                memcpy( &currentPointer,curP + PAGE_SIZE - 10, 4);
                if(currentPointer>0) {
                    memcpy(Phalf + PAGE_SIZE - 10, &currentPointer, 4);
                }
                // 1 split right
            }else  // insert part in left
            {
//                memcpy(Phalf,curP,((slotNum-1)/2)*dataSize);
//                memset(curP,0,((slotNum-1)/2)*dataSize);
//                memcpy(curP,curP+((slotNum-1)/2)*dataSize,((slotNum/2)+1)*dataSize );
//                memset(curP+((slotNum/2)+1)*dataSize,0,((slotNum-1)/2)*dataSize);
//                short newInsertIndex=insertIndex;
//                short newMigrateNums=((slotNum-1)/2) -newInsertIndex;
//                memcpy(Phalf+(newInsertIndex+1)*dataSize,Phalf+newInsertIndex*dataSize,newMigrateNums*dataSize);
//                memcpy(Phalf + newInsertIndex * dataSize, key, keySize);
//                memcpy(Phalf + newInsertIndex * dataSize + keySize , &rid.pageNum, sizeof(unsigned));
//                memcpy(Phalf + newInsertIndex * dataSize + keySize + sizeof(unsigned) , &rid.slotNum, sizeof(short));
//                short nSlotNum=(slotNum/2)+1;
//                short nfreeBytes=freeBytes+((slotNum-1)/2)*dataSize;
//                memcpy(curP + PAGE_SIZE - 2, &nfreeBytes, 2);
//                memcpy(curP + PAGE_SIZE - 4, &nSlotNum, 2);
//                dataNums=((slotNum-1)/2)+1;
//                memcpy(PinteriorKey,curP,keySize);
                memcpy(Phalf,curP+((slotNum-1)/2)*dataSize,((slotNum/2)+1)*dataSize);
                memset(curP+((slotNum-1)/2)*dataSize,0,((slotNum/2)+1)*dataSize);
                short newInsertIndex=insertIndex;
                short newMigrateNums=((slotNum-1)/2) -newInsertIndex;
                memcpy(curP+(newInsertIndex+1)*dataSize,curP+newInsertIndex*dataSize,newMigrateNums*dataSize);
                memcpy(curP + newInsertIndex * dataSize, key, keySize);
                memcpy(curP + newInsertIndex * dataSize + keySize , &rid.pageNum, sizeof(unsigned));
                memcpy(curP + newInsertIndex * dataSize + keySize + sizeof(unsigned) , &rid.slotNum, sizeof(short));
                short nSlotNum=((slotNum-1)/2)+1;
                short nfreeBytes=freeBytes+((slotNum/2)+1)*dataSize-dataSize;
                memcpy(curP + PAGE_SIZE - 2, &nfreeBytes, 2);
                memcpy(curP + PAGE_SIZE - 4, &nSlotNum, 2);
                dataNums=(slotNum/2)+1;
                memcpy(PinteriorKey,Phalf,keySize);
                int currentPointer;
                memcpy( &currentPointer,curP + PAGE_SIZE - 10, 4);
                if(currentPointer>0) {
                    memcpy(Phalf + PAGE_SIZE - 10, &currentPointer, 4);
                }
               // 0 split left
            }

            int totalNums=ixFileHandle.fileHandle.getNumberOfPages();
            int splitPageNum=totalNums;
            int originalPageNum=pageNum;


            //set append page pointer

            memcpy(curP + PAGE_SIZE - 10, &splitPageNum, 4);
            splitLeafPage(ixFileHandle, originalPageNum,splitPageNum, attribute, interiorKey, halfPage, dataNums, keySize,
                          InteriorPageNum, originalPageNum, splitPageNum);


            free(halfPage);
            free(interiorKey);
        }
        ixFileHandle.fileHandle.writePage(pageNum, readPage);       // write page to disk

        free(readPage);
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
        short dataSize=keySize+4; // 4-- right pointer
        int keyIValue=0;
        float keyFValue;
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
            string compareFirst= string(curP + 4  + 4, keySize - 4);
            if(keySValue<compareFirst){
                insertIndex = 0;
                find = true;
            }
            if(keySValue>=compareLast){insertIndex=slotNum;find=true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while(left<right)
                {
                    short mid=left+(right-left)/2;
                    string compareMid = string(curP +4+ mid * dataSize + 4, keySize - 4);
                    if(keySValue == compareMid)
                    {
                        left=mid+1;
                        break;
                    }
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
            int compareFirst;
            memcpy(&compareFirst, curP + 4 , 4);
            if (keyIValue < compareFirst) {
                insertIndex = 0;
                find = true;
            }
            if(keyIValue>=compareLast){insertIndex=slotNum;find=true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while(left<right)
                {
                    short mid=left+(right-left)/2;
                    int compareMid;
                    memcpy(&compareMid,curP +4+ mid * dataSize,4);
                    if(keyIValue == compareMid)
                    {
                        left=mid+1;
                        break;
                    }
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
            float compareFirst;
            memcpy(&compareFirst, curP + 4 , 4);
            if (keyFValue < compareFirst) {
                insertIndex = 0;
                find = true;
            }
            if(keyFValue>=compareLast){insertIndex=slotNum;find=true;}
            short left=0;
            short right=slotNum-1;
            if(find==false){
                while(left<right)
                {
                    short mid=left+(right-left)/2;
                    float compareMid;
                    memcpy(&compareMid,curP + 4 + mid * dataSize,4);
                    if(keyFValue == compareMid)
                    {
                        left=mid+1;
                        break;
                    }
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


//        if(leafPageChangeSignal==0 && insertIndex!=0) //split left leaf and not first one
//        {
//            memcpy(&Rpointer,&Lpointer,4);
//            memcpy(&Lpointer,curP+insertIndex*dataSize,4);
//            // change next pointer of formal leaf page
//            int resetLeafPageNum;
//            memcpy(&resetLeafPageNum,curP+insertIndex*dataSize,4);
//            void * resetLeafPage= malloc(PAGE_SIZE);
//            ixFileHandle.fileHandle.readPage(resetLeafPageNum,resetLeafPage);
//            memcpy((char *)resetLeafPage+PAGE_SIZE-10,&Lpointer,4);
//            ixFileHandle.fileHandle.writePage(resetLeafPageNum,resetLeafPage);
//            free(resetLeafPage);
//        }

        if ((freeBytes - dataSize)>= 0) // not split
        {
            short migrateNums=slotNum-insertIndex;    // migrate other keys backward
            memcpy(curP+(insertIndex+1)*dataSize,curP+insertIndex*dataSize,4+migrateNums*dataSize);
            memcpy(curP+insertIndex*dataSize,&Lpointer,4);
            memcpy(curP+insertIndex*dataSize+4,key,keySize);
            memcpy(curP+insertIndex*dataSize+4+keySize,&Rpointer,4);

            slotNum += 1;
            freeBytes = freeBytes - dataSize;
            memcpy(curP + PAGE_SIZE - 2, &freeBytes, 2);
            memcpy(curP + PAGE_SIZE - 4, &slotNum, 2);
        }
        else //split the interior Page
        {
            void * halfPage= malloc(PAGE_SIZE);
            void * interiorKey=malloc(keySize);
            char * Phalf=(char *)halfPage;
            char * PinteriorKey=(char *)interiorKey;
            short dataNums=0;
            if(insertIndex>=(slotNum+1)/2) // insert num in spilt part
            {
                memcpy(Phalf,curP+((slotNum+1)/2)*dataSize,4+(slotNum/2)*dataSize);
                memset(curP+4+((slotNum+1)/2)*dataSize,0,(slotNum/2)*dataSize);
                short newInsertIndex=insertIndex-(slotNum+1)/2;
                short newMigrateNums=(slotNum/2) -newInsertIndex;
                memcpy(Phalf+(newInsertIndex+1)*dataSize,Phalf+newInsertIndex*dataSize,4+newMigrateNums*dataSize);
                memcpy(Phalf+newInsertIndex*dataSize,&Lpointer,4);
                memcpy(Phalf+newInsertIndex*dataSize+4,key,keySize);
                memcpy(Phalf+newInsertIndex*dataSize+4+keySize,&Rpointer,4);
                short nSlotNum=(slotNum+1)/2;
                short nfreeBytes=freeBytes+(slotNum/2)*dataSize;
                memcpy(curP + PAGE_SIZE - 2, &nfreeBytes, 2);
                memcpy(curP + PAGE_SIZE - 4, &nSlotNum, 2);
                dataNums=(slotNum/2);
                memcpy(PinteriorKey,Phalf+4,keySize);
                memcpy(Phalf,Phalf+4+keySize,4+(slotNum/2)*dataSize);
                memset(Phalf+4+(slotNum/2)*dataSize,0,4+keySize);

            }else  // insert num in current page
            {
//                memcpy(Phalf,curP,4+((slotNum-1)/2)*dataSize);
//                memset(curP,0,((slotNum-1)/2)*dataSize);
//                memcpy(curP,curP+((slotNum-1)/2)*dataSize,4+((slotNum/2)+1)*dataSize );
//                memset(curP+4+((slotNum/2)+1)*dataSize,0,((slotNum-1)/2)*dataSize);
//                short newInsertIndex=insertIndex;
//                short newMigrateNums=((slotNum-1)/2) -newInsertIndex;
//                memcpy(Phalf+4+(insertIndex+1)*dataSize,Phalf+4+insertIndex*dataSize,newMigrateNums*dataSize);
//                memcpy(Phalf+4+insertIndex*dataSize-4,&Lpointer,4);
//                memcpy(Phalf+4+insertIndex*dataSize,key,keySize);
//                memcpy(Phalf+4+insertIndex*dataSize+keySize,&Rpointer,4);
//                short nSlotNum=slotNum/2;
//                short nfreeBytes=freeBytes+((slotNum-1)/2)*dataSize+dataSize;
//                memcpy(curP + PAGE_SIZE - 2, &nfreeBytes, 2);
//                memcpy(curP + PAGE_SIZE - 4, &nSlotNum, 2);
//                dataNums=((slotNum-1)/2)+1;
//                memcpy(PinteriorKey,curP+4,keySize);
//                memcpy(curP,curP+4+keySize,4+(slotNum/2)*dataSize);
//                memset(curP+4+(slotNum/2)*dataSize,0,4+keySize);
//                splitFlag=0;// 0 split left   , 1 split right
                memcpy(Phalf,curP+((slotNum-1)/2)*dataSize,4+((slotNum/2)+1)*dataSize);
                memset(curP+4+((slotNum-1)/2)*dataSize,0,((slotNum/2)+1)*dataSize);
                short newInsertIndex=insertIndex;
                short newMigrateNums=((slotNum-1)/2) -newInsertIndex;
                memcpy(curP+(newInsertIndex+1)*dataSize,curP+newInsertIndex*dataSize,4+newMigrateNums*dataSize);
                memcpy(curP+newInsertIndex*dataSize,&Lpointer,4);
                memcpy(curP+newInsertIndex*dataSize+4,key,keySize);
                memcpy(curP+newInsertIndex*dataSize+4+keySize,&Rpointer,4);
                short nSlotNum=((slotNum-1)/2)+1;
                short nfreeBytes=freeBytes+((slotNum/2)+1)*dataSize-dataSize;
                memcpy(curP + PAGE_SIZE - 2, &nfreeBytes, 2);
                memcpy(curP + PAGE_SIZE - 4, &nSlotNum, 2);
                dataNums=(slotNum/2)+1;
                memcpy(PinteriorKey,Phalf+4,keySize);
                memcpy(Phalf,Phalf+4+keySize,4+((slotNum/2))*dataSize);
                memset(Phalf+4+(slotNum/2)*dataSize,0,4+keySize);
            }

            int totalNums=ixFileHandle.fileHandle.getNumberOfPages();
            int splitPageNum=totalNums;
            int originalPageNum=pageNum;


            //set append page pointer

            splitInteriorPage(ixFileHandle, originalPageNum,splitPageNum, attribute, interiorKey, halfPage, dataNums, keySize,
                              InteriorPageNum, originalPageNum, splitPageNum);

            free(halfPage);
            free(interiorKey);
        }
        ixFileHandle.fileHandle.writePage(pageNum, readPage);       // write page to disk

        free(readPage);
        return 0;
    }

    RC IndexManager::splitLeafPage(IXFileHandle &ixFileHandle,PageNum originalPageNum,PageNum splitPageNum,const Attribute &attribute,const void * interiorKey,const void * halfPageData, short dataNums,short keySize,PageNum interiorPageNum, PageNum leftPointer,PageNum rightPointer)
    {
        appendAndWriteLeafPage(ixFileHandle,originalPageNum,splitPageNum,halfPageData,dataNums,keySize);
        if(interiorPageNum==4294967295) {// need to append a new parent interior page
            int totalNums=ixFileHandle.fileHandle.getNumberOfPages();
            interiorPageNum=totalNums;
            void *data = malloc(PAGE_SIZE);
            char *Pdata=(char *)data;
//            if (splitFlag==1)// 0 split left   , 1 split right
//            {
//                memcpy(Pdata,&originalPageNum,4);
//                memcpy(Pdata+4,(char *)interiorKey,keySize);
//                memcpy(Pdata+4+keySize,&splitPageNum,4);
//            }else
//            {
//                memcpy(Pdata,&splitPageNum,4);
//                memcpy(Pdata+4,(char *)interiorKey,keySize);
//                memcpy(Pdata+4+keySize,&originalPageNum,4);
//            }
            memcpy(Pdata,&leftPointer,4);
            memcpy(Pdata+4,(char *)interiorKey,keySize);
            memcpy(Pdata+4+keySize,&rightPointer,4);
            appendAndWriteInteriorPage(ixFileHandle,-1,interiorPageNum,data,1,keySize);
            resetRootPointer(ixFileHandle,interiorPageNum);
            free(data);
            return 0;
        }

        if(searchIndex==0) {
            insertEntryToInteriorPage(ixFileHandle, interiorPageNum, -1, attribute, interiorKey, keySize,
                                      leftPointer,
                                      rightPointer);
            return 0;
        }
        else
        {
            searchIndex-=1;
            int interiorParentPageNum = BtreeSearchArray[searchIndex];
            insertEntryToInteriorPage(ixFileHandle, interiorPageNum, interiorParentPageNum, attribute, interiorKey, keySize,
                                      leftPointer,
                                      rightPointer);
            return 0;
        }
        return -1;
    }


    RC IndexManager::splitInteriorPage(IXFileHandle &ixFileHandle, PageNum originalPageNum, PageNum splitPageNum,const Attribute &attribute, const void *interiorKey, const void *halfPageData,short dataNums, short keySize, PageNum interiorPageNum, PageNum leftPointer,PageNum rightPointer)
    {
        appendAndWriteInteriorPage(ixFileHandle,originalPageNum,splitPageNum,halfPageData,dataNums,keySize);
        if(interiorPageNum==4294967295) {   // need to append a new parent interior page
            int totalNums=ixFileHandle.fileHandle.getNumberOfPages();
            interiorPageNum=totalNums;

            void *data = malloc(PAGE_SIZE);
            char *Pdata=(char *)data;

            memcpy(Pdata,&originalPageNum,4);
            memcpy(Pdata+4,(char *)interiorKey,keySize);
            memcpy(Pdata+4+keySize,&splitPageNum,4);

            appendAndWriteInteriorPage(ixFileHandle,0,interiorPageNum,data,1,keySize);
            resetRootPointer(ixFileHandle,interiorPageNum);
            free(data);
            return 0;
        }
        if(searchIndex==0){
            insertEntryToInteriorPage(ixFileHandle,interiorPageNum,-1,attribute,interiorKey,keySize,leftPointer,rightPointer);
            return 0;
        }else
        {
            searchIndex-=1;
            int interiorParentPageNum = BtreeSearchArray[searchIndex];
            insertEntryToInteriorPage(ixFileHandle,interiorPageNum,interiorParentPageNum,attribute,interiorKey,keySize,leftPointer,rightPointer);
            return 0;
        }
        return -1;
    }


    RC IndexManager::resetRootPointer(IXFileHandle &ixFileHandle, PageNum rootPointerPageNum)
    {
        void *rootPage = malloc(PAGE_SIZE);

        char *curP = (char *)rootPage;
        int newPointer;
        newPointer=rootPointerPageNum;
        memcpy(curP,&newPointer,4);
        ixFileHandle.fileHandle.writePage(0, rootPage);       // write page to disk
        free(rootPage);
        return 0;
    }

    RC IndexManager::searchLeftMostEntry(IXFileHandle &ixFileHandle ,PageNum &entryPageNum, short &entryOffSet, short &entryIndex,void *leafPageBuffer)
    {
        int totalPageNum=ixFileHandle.fileHandle.getNumberOfPages();
        if(totalPageNum==0){return -1;}

        void *readRootPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(0 , readRootPage);
        char *curP = (char *)readRootPage;
        int rootPage;
        memcpy(&rootPage,curP,4);
        free(readRootPage);

        if(totalPageNum==2) {
            entryPageNum = rootPage;
            void *readPage = malloc(PAGE_SIZE);
            ixFileHandle.fileHandle.readPage(rootPage, readPage);
            char *curP = (char *) readPage;
            short slotNum;
            memcpy(&slotNum, curP+PAGE_SIZE-4, 2);
            if(slotNum==0){return -1;}
            memcpy((char *) leafPageBuffer, curP, PAGE_SIZE);
            entryOffSet=0;
            entryIndex=0;
            free(readPage);
        }else if(totalPageNum>2){
            short leafNodeChecker=0;
            int nextPageNum=rootPage;

            while(leafNodeChecker!=1) { // loop until reach the leaf page
                void *readPage = malloc(PAGE_SIZE);
                ixFileHandle.fileHandle.readPage(nextPageNum, readPage);
                char *curP = (char *) readPage;
                short slotNum;
                memcpy(&slotNum, curP+PAGE_SIZE-4, 2);
                if(slotNum==0){return -1;}
                memcpy(&leafNodeChecker, curP + PAGE_SIZE - 6, 2);
                if(leafNodeChecker==1){
                    entryPageNum=nextPageNum;
                    memcpy((char *)leafPageBuffer,curP,PAGE_SIZE);
                    entryOffSet=0;
                    entryIndex=0;
                    free(readPage);
                    break;
                }

                int tempPage=0;

                memcpy(&tempPage, curP , 4);

                free(readPage);
                nextPageNum=tempPage;
            }
        }
        return 0;
    }

    RC IndexManager::searchEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,PageNum &entryPageNum, void *leafPageBuffer)
    {
        int totalPageNum=ixFileHandle.fileHandle.getNumberOfPages();
        if(totalPageNum==0){return -1;}

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
        float keyFValue;
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

        void *readRootPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(0 , readRootPage);
        char *curP = (char *)readRootPage;
        int rootPage;
        memcpy(&rootPage,curP,4);
        free(readRootPage);

        if(totalPageNum==2)
        {
            entryPageNum=rootPage;
            void *readPage = malloc(PAGE_SIZE);
            ixFileHandle.fileHandle.readPage(rootPage, readPage);
            curP = (char *) readPage;
            memcpy((char *)leafPageBuffer,curP,PAGE_SIZE);
            free(readPage);
        }
        else if(totalPageNum>2){
            short leafNodeChecker=0;
            int nextPageNum=rootPage;

            while(leafNodeChecker!=1) { // loop until reach the leaf page
                void *readPage = malloc(PAGE_SIZE);
                ixFileHandle.fileHandle.readPage(nextPageNum, readPage);
                curP = (char *) readPage;
                memcpy(&leafNodeChecker, curP + PAGE_SIZE - 6, 2);
                short freeBytes;
                short slotNum;
                memcpy(&freeBytes,curP+PAGE_SIZE-2,2);
                memcpy(&slotNum,curP+PAGE_SIZE-4,2);
                if(leafNodeChecker==1){
                    entryPageNum=nextPageNum;
                    memcpy((char *)leafPageBuffer,curP,PAGE_SIZE);
                    break;
                }

                short dataSize = keySize + 4;
                short insertIndex;
                int tempPage=0;
                if (attribute.type == TypeVarChar) {
                    bool find = false;
                    string compareLast = string(curP + 4 + (slotNum - 1) * dataSize + 4, keySize - 4);
                    string compareFirst= string(curP + 4  + 4, keySize - 4);
                    if(keySValue<=compareFirst){
                        insertIndex = 0;
                        find = true;
                    }
                    if (keySValue > compareLast) {
                        insertIndex = slotNum;
                        find = true;
                    }
                    short left = 0;
                    short right = slotNum - 1;
                    short leftmost=-1;
                    short mid;
                    if (find == false) {
                        while (left <= right) {
                            mid = left + ((right - left) >> 1);
                            string compareMid = string(curP + 4 + mid * dataSize + 4, keySize - 4);
                            if (keySValue <= compareMid) {
                                leftmost = mid;
                                right=mid-1;
                            } else {
                                left = mid+1;
                            }
                        }
                        insertIndex = leftmost;
                    }
                } else if (attribute.type == TypeInt) {
                    bool find = false;
                    int compareLast;
                    memcpy(&compareLast, curP + 4 + (slotNum - 1) * dataSize, 4);
                    int compareFirst;
                    memcpy(&compareFirst, curP + 4 , 4);
                    if (keyIValue <= compareFirst) {
                        insertIndex = 0;
                        find = true;
                    }
                    if (keyIValue > compareLast) {
                        insertIndex = slotNum;
                        find = true;
                    }
                    short left = 0;
                    short right = slotNum - 1;
                    short leftmost=-1;
                    short mid;
                    if (find == false) {
                        while (left <= right) {
                            mid = left + ((right - left) >> 1);
                            int compareMid;
                            memcpy(&compareMid, curP + 4 + mid * dataSize, 4);
                            if(keyIValue <= compareMid)
                            {
                                leftmost = mid;
                                right=mid-1;
                            } else {
                                left = mid+1;
                            }
                        }
                        insertIndex = leftmost;
                    }

                } else {
                    bool find = false;
                    float compareLast;
                    memcpy(&compareLast, curP + 4 + (slotNum - 1) * dataSize, 4);
                    float compareFirst;
                    memcpy(&compareFirst, curP + 4 , 4);
                    if (keyFValue <= compareFirst) {
                        insertIndex = 0;
                        find = true;
                    }
                    if (keyFValue > compareLast) {
                        insertIndex = slotNum;
                        find = true;
                    }
                    short left = 0;
                    short right = slotNum - 1;
                    short leftmost=-1;
                    short mid;
                    if (find == false) {
                        while (left <= right) {
                            mid = left + ((right - left) >> 1);
                            float compareMid;
                            memcpy(&compareMid, curP + 4 + mid * dataSize, 4);
                            if(keyFValue <= compareMid)
                            {
                                leftmost = mid;
                                right=mid-1;
                            } else {
                                left = mid+1;
                            }
                        insertIndex = leftmost;
                        }
                    }
               }
                if(insertIndex==-1){return -1;}
                memcpy(&tempPage, curP + insertIndex * dataSize, 4);
                free(readPage);
                nextPageNum=tempPage;
            }
        }
        return 0;
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {

        short keySize=0;
        if(attribute.type==TypeReal || attribute.type==TypeInt)
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
        float keyFValue;
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

        PageNum deletePageNum;
        short deleteIndex=0;
        short deleteOffset=0;
        unsigned deleteRidPN=-1;
        short deleteRidSN=-1;
        void* pageBuffer= malloc(PAGE_SIZE);
        RC rc= searchEntry(ixFileHandle,attribute,key,deletePageNum,pageBuffer);
        if(rc==-1){return -1;}
        char *curP=(char *)pageBuffer;
        short slotNum;
        memcpy(&slotNum,curP+PAGE_SIZE-4,2);
        // this is the first identical key
        if(slotNum==0){
            memcpy(&deletePageNum, curP + PAGE_SIZE - 10, 4);
            ixFileHandle.fileHandle.readPage(deletePageNum, pageBuffer);
            deleteIndex = 0;
            deleteOffset = 0;
            curP = (char *) pageBuffer;
            memcpy(&slotNum,curP+PAGE_SIZE-4,2);
        }
        memcpy(&deleteRidPN,curP+deleteOffset+keySize,sizeof(unsigned));
        memcpy(&deleteRidSN,curP+deleteOffset+keySize+sizeof(unsigned),sizeof(short));
        short deleteDataSize=keySize+sizeof(unsigned)+sizeof(short);
        bool findDelete=false;

        if(attribute.type==TypeInt) {
            int deleteIKey;
            memcpy(&deleteIKey,curP+deleteOffset,4);
            if(deleteIKey==keyIValue && rid.pageNum == deleteRidPN && rid.slotNum == deleteRidSN)
            {
                findDelete= true;
            }
            while (deleteIKey!=keyIValue || rid.pageNum != deleteRidPN || rid.slotNum != deleteRidSN) {
                if (deleteIndex == slotNum - 1) {
                    int nextPageNum;
                    memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                    if(nextPageNum!=-1){
                        memcpy(&deletePageNum, curP + PAGE_SIZE - 10, 4);
                        ixFileHandle.fileHandle.readPage(deletePageNum, pageBuffer);
                        deleteIndex = 0;
                        deleteOffset = 0;
                    }else
                    {
                        break;
                    }
                } else {
                    deleteIndex += 1;
                    deleteOffset = deleteOffset + deleteDataSize;
                }
                curP = (char *) pageBuffer;
                memcpy(&deleteIKey,curP+deleteOffset,4);
                if(deleteIKey>keyIValue){break;}
                memcpy(&slotNum, curP + PAGE_SIZE - 4, 2);
                memcpy(&deleteRidPN, curP + deleteOffset + keySize, sizeof(unsigned));
                memcpy(&deleteRidSN, curP + deleteOffset + keySize + sizeof(unsigned), sizeof(short));
                if(deleteIKey==keyIValue && rid.pageNum == deleteRidPN && rid.slotNum == deleteRidSN)
                {
                    findDelete= true;
                }
            }
        }else if(attribute.type==TypeReal)
        {
            float deleteFKey;
            memcpy(&deleteFKey,curP+deleteOffset,4);
            if(deleteFKey==keyFValue && rid.pageNum == deleteRidPN && rid.slotNum == deleteRidSN)
            {
                findDelete= true;
            }
            while (deleteFKey!=keyFValue || rid.pageNum != deleteRidPN || rid.slotNum != deleteRidSN) {
                if (deleteIndex == slotNum - 1) {
                    int nextPageNum;
                    memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                    if(nextPageNum!=-1){
                        memcpy(&deletePageNum, curP + PAGE_SIZE - 10, 4);
                        ixFileHandle.fileHandle.readPage(deletePageNum, pageBuffer);
                        deleteIndex = 0;
                        deleteOffset = 0;
                    }else
                    {
                        break;
                    }
                } else {
                    deleteIndex += 1;
                    deleteOffset = deleteOffset + deleteDataSize;
                }
                curP = (char *) pageBuffer;
                memcpy(&deleteFKey,curP+deleteOffset,4);
                if(deleteFKey>keyFValue){break;}
                memcpy(&slotNum, curP + PAGE_SIZE - 4, 2);
                memcpy(&deleteRidPN, curP + deleteOffset + keySize, sizeof(unsigned));
                memcpy(&deleteRidSN, curP + deleteOffset + keySize + sizeof(unsigned), sizeof(short));
                if(deleteFKey==keyFValue && rid.pageNum == deleteRidPN && rid.slotNum == deleteRidSN)
                {
                    findDelete= true;
                }
            }
        }else{
            string deleteSKey;
            int varcharLength;
            memcpy(&varcharLength,curP+deleteOffset,4);
            deleteSKey=string (curP+deleteOffset+4,varcharLength);
            if(deleteSKey==keySValue && rid.pageNum == deleteRidPN && rid.slotNum == deleteRidSN)
            {
                findDelete= true;
            }
            while (deleteSKey!=keySValue || rid.pageNum != deleteRidPN || rid.slotNum != deleteRidSN) {
                if (deleteIndex == slotNum - 1) {
                    int nextPageNum;
                    memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                    if(nextPageNum!=-1){
                        memcpy(&deletePageNum, curP + PAGE_SIZE - 10, 4);
                        ixFileHandle.fileHandle.readPage(deletePageNum, pageBuffer);
                        deleteIndex = 0;
                        deleteOffset = 0;
                    }else
                    {
                        break;
                    }
                } else {
                    deleteIndex += 1;
                    deleteOffset = deleteOffset + deleteDataSize;
                }
                curP = (char *) pageBuffer;
                memcpy(&slotNum, curP + PAGE_SIZE - 4, 2);
                int varcharLength;
                memcpy(&varcharLength,curP+deleteOffset,4);
                deleteSKey=string (curP+deleteOffset+4,varcharLength);
                if(deleteSKey>keySValue){break;}
                memcpy(&deleteRidPN, curP + deleteOffset + keySize, sizeof(unsigned));
                memcpy(&deleteRidSN, curP + deleteOffset + keySize + sizeof(unsigned), sizeof(short));
                if(deleteSKey==keySValue && rid.pageNum == deleteRidPN && rid.slotNum == deleteRidSN)
                {
                    findDelete= true;
                }
            }
        }
        if(findDelete==true) {
            short freeBytes;
            memcpy(&freeBytes, curP + PAGE_SIZE - 2, 2);
            short migrateSize = 4086 - freeBytes - deleteOffset - deleteDataSize;
            memcpy(curP + deleteOffset, curP +deleteOffset+deleteDataSize, migrateSize);
            memset(curP + deleteOffset + migrateSize, 0, deleteDataSize);
            short slotNum1;
            memcpy(&slotNum1, curP + PAGE_SIZE - 4, 2);
            slotNum1 = slotNum1 - 1;
            freeBytes = freeBytes + deleteDataSize;
            memcpy(curP + PAGE_SIZE - 2, &freeBytes, 2);
            memcpy(curP + PAGE_SIZE - 4, &slotNum1, 2);
            ixFileHandle.fileHandle.writePage(deletePageNum, pageBuffer);
        }
        free(pageBuffer);
        if(findDelete== false){return -1;}
        return 0;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        if(ixFileHandle.fileHandle.getFileP()==nullptr){
            return -1;
        }
        RC rc=ix_ScanIterator.initScanIterator(ixFileHandle, attribute, lowKey, highKey,
                                   lowKeyInclusive, highKeyInclusive, this);
        if(rc==-1){return -1;}
        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        int totalPageNum=ixFileHandle.fileHandle.getNumberOfPages();
        if(totalPageNum==0)
        {
            out<<"{}";
            return 0;
        }else
        {
            void *readRootPage = malloc(PAGE_SIZE);
            ixFileHandle.fileHandle.readPage(0 , readRootPage);
            char *curP = (char *)readRootPage;
            int rootPage;
            memcpy(&rootPage,curP,4);
            ixFileHandle.fileHandle.writePage(0, readRootPage);       // write page to disk
            free(readRootPage);
            printPage(ixFileHandle,attribute,rootPage,out);
        }
    }

    RC IndexManager::printPage(IXFileHandle &ixFileHandle, const Attribute &attribute, PageNum pageNum,std::ostream &out) const
    {
        void *readPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(pageNum, readPage);
        char *curP = (char *)readPage;
        short slotNum;
        memcpy(&slotNum,curP+PAGE_SIZE-4,2);
        short leafCheker=0;
        memcpy(&leafCheker,curP+PAGE_SIZE-6,2);
        if(leafCheker==1)
        { //leaf node
            out<<"{\"keys\": [";
            if(attribute.type==TypeInt)
            {
                int curKeyIvalue;
                int prevKeyIvalue;
                short dataSize=4+ sizeof(unsigned) +sizeof(short);
                for (int i=0;i<slotNum;i++){
                    memcpy(&curKeyIvalue,curP+i*dataSize,4);
                    unsigned ridPageNum;
                    memcpy(&ridPageNum,curP+i*dataSize+4,sizeof(unsigned));
                    unsigned short ridSlotNum;
                    memcpy(&ridSlotNum,curP+i*dataSize+4+sizeof(unsigned),sizeof(short));

                    if (i != 0 && curKeyIvalue != prevKeyIvalue) out << "]\",";
                    if (i == 0 || curKeyIvalue != prevKeyIvalue)
                    {
                        out<<"\""<<curKeyIvalue<<":[("<<ridPageNum<<","<<ridSlotNum<<")";
                    }
                    else
                    {
                        out << ",(" << ridPageNum << "," << ridSlotNum << ")";
                    }
                    if (i == slotNum-1) out << "]\"";
                    prevKeyIvalue = curKeyIvalue;
                }
            }
            else if(attribute.type==TypeReal)
            {
                float curKeyFvalue;
                float prevKeyFvalue;
                short dataSize=4+ sizeof(RID);
                for (int i=0;i<slotNum;i++){
                    memcpy(&curKeyFvalue,curP+i*dataSize,4);
                    unsigned ridPageNum;
                    memcpy(&ridPageNum,curP+i*dataSize+4,sizeof(unsigned));
                    unsigned short ridSlotNum;
                    memcpy(&ridSlotNum,curP+i*dataSize+4+sizeof(unsigned),sizeof(short));

                    if (i != 0 && curKeyFvalue != prevKeyFvalue) out << "]\",";
                    if (i == 0 || curKeyFvalue != prevKeyFvalue)
                    {
                        out<<"\""<<curKeyFvalue<<":[("<<ridPageNum<<","<<ridSlotNum<<")";
                    }
                    else
                    {
                        out << ",(" << ridPageNum << "," << ridSlotNum << ")";
                    }
                    if (i == slotNum-1) out << "]\"";
                    prevKeyFvalue=curKeyFvalue;
                }
            }
            else
            {
                string curKeySvalue;
                string prevKeySvalue;

                int varCharLength;
                memcpy(&varCharLength,curP,4);
                short dataSize=4+ varCharLength+sizeof(RID);
                for (int i=0;i<slotNum;i++){
                    curKeySvalue=string (curP+i*dataSize+4,varCharLength);
                    unsigned ridPageNum;
                    memcpy(&ridPageNum,curP+i*dataSize+4+varCharLength,sizeof(unsigned));
                    unsigned short ridSlotNum;
                    memcpy(&ridSlotNum,curP+i*dataSize+4+varCharLength+sizeof(unsigned),sizeof(short));

                    if (i != 0 && curKeySvalue != prevKeySvalue) out << "]\",";
                    if (i == 0 || curKeySvalue != prevKeySvalue)
                    {
                        out<<"\""<<curKeySvalue<<":[("<<ridPageNum<<","<<ridSlotNum<<")";
                    }
                    else
                    {
                        out << ",(" << ridPageNum << "," << ridSlotNum << ")";
                    }
                    if (i == slotNum-1) out << "]\"";
                    prevKeySvalue=curKeySvalue;
                }
            }
            out << "]}";
        }
        else
        {
            vector<int> childPointerArray;
            out << "{\"keys\": [";
            if(attribute.type==TypeInt)
            {
                int curKeyIvalue;
                short dataSize=4+4;
                int curChildPointer;
                memcpy(&curChildPointer,curP,4);
                childPointerArray.push_back(curChildPointer);
                for(int i=0;i<slotNum;i++)
                {
                    memcpy(&curKeyIvalue,curP+4+i*dataSize,4);
                    memcpy(&curChildPointer,curP+4+i*dataSize+4,4);
                    childPointerArray.push_back(curChildPointer);
                    out << "\"" << curKeyIvalue << "\"";
                    if (i == slotNum - 1) {
                        out << "],\n";
                        out << "\"children\": [\n";
                    }
                    else out << ",";
                }
            }
            else if(attribute.type==TypeReal)
            {
                float curKeyFvalue;
                short dataSize=4+4;
                int curChildPointer;
                memcpy(&curChildPointer,curP,4);
                childPointerArray.push_back(curChildPointer);
                for(int i=0;i<slotNum;i++)
                {
                    memcpy(&curKeyFvalue,curP+4+i*dataSize,4);
                    memcpy(&curChildPointer,curP+4+i*dataSize+4,4);
                    childPointerArray.push_back(curChildPointer);
                    out << "\"" << curKeyFvalue << "\"";
                    if (i == slotNum - 1) {
                        out << "],\n";
                        out << "\"children\": [\n";
                    }
                    else out << ",";
                }
            }
            else
            {
                string curKeySvalue;
                int varCharLength;
                memcpy(&varCharLength,curP+4,4);
                short dataSize=4+4+varCharLength;
                int curChildPointer;
                memcpy(&curChildPointer,curP,4);
                childPointerArray.push_back(curChildPointer);
                for(int i=0;i<slotNum;i++)
                {
                    curKeySvalue=string(curP+4+i*dataSize+4,varCharLength);
                    memcpy(&curChildPointer,curP+4+i*dataSize+4+varCharLength,4);
                    childPointerArray.push_back(curChildPointer);
                    out << "\"" << curKeySvalue << "\"";
                    if (i == slotNum - 1) {
                        out << "],\n";
                        out << "\"children\": [\n";
                    }
                    else out << ",";
                }
            }

            for(int childPointer : childPointerArray)
            {
                printPage(ixFileHandle,attribute,childPointer,out);
                if (childPointer != childPointerArray[childPointerArray.size()-1]) out << ",\n" ;
                else out << "\n";
            }
            out << "]}";
        }
        ixFileHandle.fileHandle.writePage(pageNum, readPage);       // write page to disk
        free(readPage);
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::initScanIterator(IXFileHandle &ixFileHandle,const Attribute &attribute, const void *lowKey,const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,IndexManager *ix)
    {
        this->ixFileHandle=&ixFileHandle;
        this->attribute=attribute;
        this->lowKey=lowKey;
        this->highKey=highKey;
        this->highKeyInclusive=highKeyInclusive;
        this->lowKeyInclusive=lowKeyInclusive;
        this->ix=ix;
        this->curPageNum=-1; // loop end ==-2
        this->curOffSet=0;
        this->curIndex=0;
        this->curPageBuffer= malloc(PAGE_SIZE);
        this->curSlotNum=0;

        return 0;
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        short lowKeySize = 0;
        int lowKeyIValue = 0;
        float lowKeyFValue;
        string lowKeySValue;
        if(lowKey!=NULL) {
            if (this->attribute.type == TypeReal || this->attribute.type == TypeInt) {
                lowKeySize += 4;
            }
            if (this->attribute.type == TypeVarChar) {
                int varCharLength = 0;
                memcpy(&varCharLength, (char *) lowKey, 4);
                lowKeySize += 4;
                lowKeySize += varCharLength;
            }
            if (this->attribute.type == TypeInt) {
                memcpy(&lowKeyIValue, (char *) lowKey, 4);
            } else if (this->attribute.type == TypeReal) {
                memcpy(&lowKeyFValue, (char *) lowKey, 4);
            } else {
                lowKeySValue = string((char *) lowKey + 4, lowKeySize - 4);
            }
        }
        short highKeySize = 0;
        int highKeyIValue = 0;
        float highKeyFValue;
        string highKeySValue;
        if(highKey!=NULL) {
            if (this->attribute.type == TypeReal || this->attribute.type == TypeInt) {
                highKeySize += 4;
            }
            if (this->attribute.type == TypeVarChar) {
                int varCharLength = 0;
                memcpy(&varCharLength, (char *) highKey, 4);
                highKeySize += 4;
                highKeySize += varCharLength;
            }

            if (this->attribute.type == TypeInt) {
                memcpy(&highKeyIValue, (char *) highKey, 4);
            } else if (this->attribute.type == TypeReal) {
                memcpy(&highKeyFValue, (char *) highKey, 4);
            } else {
                highKeySValue = string((char *) highKey + 4, highKeySize - 4);
            }
        }


        while(this->curPageNum!=-2 ) {
            if(this->curPageNum==-1) {
                if (lowKey != NULL) {
                    ix->searchEntry(*ixFileHandle, this->attribute, lowKey, this->curPageNum,this->curPageBuffer);
                    char *curP = (char *) curPageBuffer;
                    memcpy(&curSlotNum, curP + PAGE_SIZE - 4, 2);
                    curOffSet=0;
                    curIndex=0;
                    bool find=false;

                    if(attribute.type==TypeInt) {
                        int deleteIKey;
                        memcpy(&deleteIKey,curP+curOffSet,4);
                        if(deleteIKey==lowKeyIValue)
                        {
                            find= true;
                        }
                        while (deleteIKey!=lowKeyIValue) {
                            if (curIndex == curSlotNum - 1) {
                                int nextPageNum;
                                memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                                if(nextPageNum!=-1){
                                    curPageNum=nextPageNum;
                                    ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                    curIndex = 0;
                                    curOffSet = 0;
                                }else
                                {
                                    break;
                                }
                            } else {
                                curIndex += 1;
                                curOffSet = curOffSet +lowKeySize+sizeof(unsigned)+sizeof(short) ;
                            }
                            curP = (char *) curPageBuffer;
                            memcpy(&deleteIKey,curP+curOffSet,4);
                            if(deleteIKey>lowKeyIValue){break;}
                            memcpy(&curSlotNum, curP + PAGE_SIZE - 4, 2);
                            if(deleteIKey==lowKeyIValue )
                            {
                                find= true;
                            }
                        }
                    }else if(attribute.type==TypeReal)
                    {
                        float deleteFKey;
                        memcpy(&deleteFKey,curP+curOffSet,4);
                        if(deleteFKey==lowKeyFValue)
                        {
                            find= true;
                        }
                        while (deleteFKey!=lowKeyFValue) {
                            if (curIndex == curSlotNum - 1) {
                                int nextPageNum;
                                memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                                if(nextPageNum!=-1){
                                    curPageNum=nextPageNum;
                                    ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                    curIndex = 0;
                                    curOffSet = 0;
                                }else
                                {
                                    break;
                                }
                            } else {
                                curIndex += 1;
                                curOffSet = curOffSet +lowKeySize+sizeof(unsigned)+sizeof(short) ;
                            }
                            curP = (char *) curPageBuffer;
                            memcpy(&deleteFKey,curP+curOffSet,4);
                            if(deleteFKey>lowKeyFValue){break;}
                            memcpy(&curSlotNum, curP + PAGE_SIZE - 4, 2);
                            if(deleteFKey==lowKeyFValue)
                            {
                                find= true;
                            }
                        }
                    }else{
                        string deleteSKey;
                        int varcharLength;
                        memcpy(&varcharLength,curP+curOffSet,4);
                        deleteSKey=string (curP+curOffSet+4,varcharLength);
                        if(deleteSKey==lowKeySValue)
                        {
                            find= true;
                        }
                        while (deleteSKey!=lowKeySValue ) {
                            if (curIndex == curSlotNum - 1) {
                                int nextPageNum;
                                memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                                if(nextPageNum!=-1){
                                    curPageNum=nextPageNum;
                                    ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                    curIndex = 0;
                                    curOffSet = 0;
                                }else
                                {
                                    break;
                                }
                            } else {
                                curIndex += 1;
                                curOffSet = curOffSet+lowKeySize+sizeof(unsigned)+sizeof(short) ;
                            }
                            curP = (char *) curPageBuffer;
                            memcpy(&curSlotNum, curP + PAGE_SIZE - 4, 2);
                            int varcharLength;
                            memcpy(&varcharLength,curP+curOffSet,4);
                            deleteSKey=string (curP+curOffSet+4,varcharLength);
                            if(deleteSKey>lowKeySValue){break;}
                            if(deleteSKey==lowKeySValue )
                            {
                                find= true;
                            }
                        }
                    }
                    if(find==false){return IX_EOF;}
                    if (this->lowKeyInclusive == true) {
                        memcpy((char *) key, curP+curOffSet, lowKeySize);
                        memcpy(&rid.pageNum,curP+curOffSet+lowKeySize,sizeof(unsigned));
                        memcpy(&rid.slotNum,curP+curOffSet+lowKeySize+sizeof(unsigned),sizeof(short));

                        if(curIndex==curSlotNum-1)
                        {
                            int nextPageNum;
                            memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                            if(nextPageNum!=-1)
                            {
                                curPageNum=nextPageNum;
                                ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                curOffSet=0;
                                curIndex=0;
                                memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                            }else
                            {
                                curPageNum=-2; // end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }
                        else
                        {
                            curIndex+=1;
                            curOffSet=curOffSet+lowKeySize+sizeof(unsigned)+sizeof(short);
                        }
                        return 0;
                    }
                    else
                    {
                        if(curIndex==curSlotNum-1)
                        {
                            int nextPageNum;
                            memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                            if(nextPageNum!=-1)
                            {
                                curPageNum=nextPageNum;
                                ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                curOffSet=0;
                                curIndex=0;
                                memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                            }else
                            {
                                curPageNum=-2; // end while loop
                            }
                        }
                        else
                        {
                            curIndex+=1;
                            curOffSet=curOffSet+lowKeySize+sizeof(unsigned)+sizeof(short);
                        }
                    }

                }
                else{//lowkey ==NULL
                    RC rc =ix->searchLeftMostEntry(*ixFileHandle,this->curPageNum,this->curOffSet,this->curIndex,this->curPageBuffer);
                    if(rc==-1){return IX_EOF;}
                    char *curP = (char *) curPageBuffer;
                    memcpy(&curSlotNum, curP + PAGE_SIZE - 4, 2);
                    if(curSlotNum==0){return IX_EOF;}
                    if (attribute.type == TypeInt) {
                        memcpy((char *) key, curP+curOffSet, 4);
                        memcpy(&rid.pageNum, curP + curOffSet + 4, sizeof(unsigned));
                        memcpy(&rid.slotNum, curP + curOffSet + 4 + sizeof(unsigned), sizeof(short));
                        if(curIndex==curSlotNum-1){
                            int nextPageNum;
                            memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                            if(nextPageNum!=-1)
                            {
                                curPageNum=nextPageNum;
                                ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                curOffSet=0;
                                curIndex=0;
                                memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                            }else
                            {
                                curPageNum=-2; // end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }else
                        {
                            curIndex+=1;
                            curOffSet=curOffSet+4+sizeof(unsigned)+sizeof(short);
                        }
                        return 0;
                    }else if(attribute.type==TypeReal){
                        float keyFloatvalue;
                        memcpy(&keyFloatvalue,  curP+curOffSet, 4);
                        memcpy((char *)key,  &keyFloatvalue, 4);
                        memcpy(&rid.pageNum, curP + curOffSet + 4, sizeof(unsigned));
                        memcpy(&rid.slotNum, curP + curOffSet + 4 + sizeof(unsigned), sizeof(short));
                        if(curIndex==curSlotNum-1){
                            int nextPageNum;
                            memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                            if(nextPageNum!=-1)
                            {
                                curPageNum=nextPageNum;
                                ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                curOffSet=0;
                                curIndex=0;
                                memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                            }else
                            {
                                curPageNum=-2; // end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }else
                        {
                            curIndex+=1;
                            curOffSet=curOffSet+4+sizeof(unsigned)+sizeof(short);
                        }
                        return 0;
                    }else{ //attribute==varchar
                        int varCharLength;
                        memcpy(&varCharLength, curP + curOffSet, 4);

                        memcpy((char *)key, curP+curOffSet, 4);
                        memcpy((char *)key+4, curP+curOffSet+4, varCharLength);
                        memcpy(&rid.pageNum, curP + curOffSet + 4+varCharLength, sizeof(unsigned));
                        memcpy(&rid.slotNum, curP + curOffSet + 4 +varCharLength+ sizeof(unsigned), sizeof(short));
                        if(curIndex==curSlotNum-1){
                            int nextPageNum;
                            memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                            if(nextPageNum!=-1)
                            {
                                curPageNum=nextPageNum;
                                ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                curOffSet=0;
                                curIndex=0;
                                memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                            }else
                            {
                                curPageNum=-2; // end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }else
                        {
                            curIndex+=1;
                            curOffSet=curOffSet+4+varCharLength+sizeof(unsigned)+sizeof(short);
                        }
                        return 0;
                    }
                }
            }
            else{//  curPageNum!=-1
                char* curP=(char *)curPageBuffer;
                if(highKey!=NULL){
                    if(highKeyInclusive==true){
                        if (attribute.type == TypeInt) {
                            int keyIValue;
                            memcpy(&keyIValue, curP + curOffSet, 4);
                            if (keyIValue <= highKeyIValue) {
                                memcpy((char *) key, curP+curOffSet, 4);
                                memcpy(&rid.pageNum, curP + curOffSet + 4, sizeof(unsigned));
                                memcpy(&rid.slotNum, curP + curOffSet + 4 + sizeof(unsigned), sizeof(short));
                                if(curIndex==curSlotNum-1){
                                    int nextPageNum;
                                    memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                                    if(nextPageNum!=-1)
                                    {
                                        curPageNum=nextPageNum;
                                        ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                        curOffSet=0;
                                        curIndex=0;
                                        memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                                    }else
                                    {
                                        curPageNum=-2; // end while loop
                                        curOffSet=0;
                                        curIndex=0;
                                    }
                                }else
                                {
                                    curIndex+=1;
                                    curOffSet=curOffSet+4+sizeof(unsigned)+sizeof(short);
                                }
                                return 0;
                            }
                            else
                            {
                                curPageNum=-2;// end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }
                        else if(attribute.type==TypeReal){
                            float keyFValue;
                            memcpy(&keyFValue, curP + curOffSet, 4);
                            if (keyFValue <= highKeyFValue) {
                                memcpy((char *) key,  curP+curOffSet, 4);
                                memcpy(&rid.pageNum, curP + curOffSet + 4, sizeof(unsigned));
                                memcpy(&rid.slotNum, curP + curOffSet + 4 + sizeof(unsigned), sizeof(short));
                                if(curIndex==curSlotNum-1){
                                    int nextPageNum;
                                    memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                                    if(nextPageNum!=-1)
                                    {
                                        curPageNum=nextPageNum;
                                        ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                        curOffSet=0;
                                        curIndex=0;
                                        memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                                    }else
                                    {
                                        curPageNum=-2; // end while loop
                                        curOffSet=0;
                                        curIndex=0;
                                    }
                                }else
                                {
                                    curIndex+=1;
                                    curOffSet=curOffSet+4+sizeof(unsigned)+sizeof(short);
                                }
                                return 0;
                            }
                            else
                            {
                                curPageNum=-2;// end while loop
                                curOffSet=0;
                                curIndex=0;
                            }

                        }
                        else{ //attribute==varchar
                            string keySValue;
                            int varCharLength;
                            memcpy(&varCharLength, curP + curOffSet, 4);
                            keySValue=string (curP+curOffSet+4,varCharLength);
                            if (keySValue <= highKeySValue) {
                                memcpy((char *) key, curP+curOffSet, 4);
                                memcpy((char *) key+4, curP+curOffSet+4, varCharLength);
                                memcpy(&rid.pageNum, curP + curOffSet + 4+varCharLength, sizeof(unsigned));
                                memcpy(&rid.slotNum, curP + curOffSet + 4 +varCharLength+ sizeof(unsigned), sizeof(short));
                                if(curIndex==curSlotNum-1){
                                    int nextPageNum;
                                    memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                                    if(nextPageNum!=-1)
                                    {
                                        curPageNum=nextPageNum;
                                        ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                        curOffSet=0;
                                        curIndex=0;
                                        memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                                    }else
                                    {
                                        curPageNum=-2; // end while loop
                                        curOffSet=0;
                                        curIndex=0;
                                    }
                                }else
                                {
                                    curIndex+=1;
                                    curOffSet=curOffSet+4+varCharLength+sizeof(unsigned)+sizeof(short);
                                }
                                return 0;
                            }
                            else
                            {
                                curPageNum=-2;// end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }
                    }else{//not inclusive
                        if (attribute.type == TypeInt) {
                            int keyIValue;
                            memcpy(&keyIValue, curP + curOffSet, 4);
                            if (keyIValue < highKeyIValue) {
                                memcpy((char *) key, curP+curOffSet, 4);
                                memcpy(&rid.pageNum, curP + curOffSet + 4, sizeof(unsigned));
                                memcpy(&rid.slotNum, curP + curOffSet + 4 + sizeof(unsigned), sizeof(short));
                                if(curIndex==curSlotNum-1){
                                    int nextPageNum;
                                    memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                                    if(nextPageNum!=-1)
                                    {
                                        curPageNum=nextPageNum;
                                        ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                        curOffSet=0;
                                        curIndex=0;
                                        memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                                    }else
                                    {
                                        curPageNum=-2; // end while loop
                                        curOffSet=0;
                                        curIndex=0;
                                    }
                                }else
                                {
                                    curIndex+=1;
                                    curOffSet=curOffSet+4+sizeof(unsigned)+sizeof(short);
                                }
                                return 0;
                            }
                            else
                            {
                                curPageNum=-2;// end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }
                        else if(attribute.type==TypeReal){
                            float keyFValue;
                            memcpy(&keyFValue, curP + curOffSet, 4);
                            if (keyFValue < highKeyFValue) {
                                memcpy((char *) key,  curP+curOffSet, 4);
                                memcpy(&rid.pageNum, curP + curOffSet + 4, sizeof(unsigned));
                                memcpy(&rid.slotNum, curP + curOffSet + 4 + sizeof(unsigned), sizeof(short));
                                if(curIndex==curSlotNum-1){
                                    int nextPageNum;
                                    memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                                    if(nextPageNum!=-1)
                                    {
                                        curPageNum=nextPageNum;
                                        ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                        curOffSet=0;
                                        curIndex=0;
                                        memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                                    }else
                                    {
                                        curPageNum=-2; // end while loop
                                        curOffSet=0;
                                        curIndex=0;
                                    }
                                }else
                                {
                                    curIndex+=1;
                                    curOffSet=curOffSet+4+sizeof(unsigned)+sizeof(short);
                                }
                                return 0;
                            }
                            else
                            {
                                curPageNum=-2;// end while loop
                                curOffSet=0;
                                curIndex=0;
                            }

                        }
                        else{ //attribute==varchar
                            string keySValue;
                            int varCharLength;
                            memcpy(&varCharLength, curP + curOffSet, 4);
                            keySValue=string (curP+curOffSet+4,varCharLength);
                            if (keySValue < highKeySValue) {
                                memcpy((char *)key, curP+curOffSet, 4);
                                memcpy((char *)key+4, curP+curOffSet+4, varCharLength);
                                memcpy(&rid.pageNum, curP + curOffSet + 4+varCharLength, sizeof(unsigned));
                                memcpy(&rid.slotNum, curP + curOffSet + 4 +varCharLength+ sizeof(unsigned), sizeof(short));
                                if(curIndex==curSlotNum-1){
                                    int nextPageNum;
                                    memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                                    if(nextPageNum!=-1)
                                    {
                                        curPageNum=nextPageNum;
                                        ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                        curOffSet=0;
                                        curIndex=0;
                                        memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                                    }else
                                    {
                                        curPageNum=-2; // end while loop
                                        curOffSet=0;
                                        curIndex=0;
                                    }
                                }else
                                {
                                    curIndex+=1;
                                    curOffSet=curOffSet+4+varCharLength+sizeof(unsigned)+sizeof(short);
                                }
                                return 0;
                            }
                            else
                            {
                                curPageNum=-2;// end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }
                    }
                }else{// highkey==NULL
                    if (attribute.type == TypeInt) {
                        memcpy((char *) key, curP+curOffSet, 4);
                        memcpy(&rid.pageNum, curP + curOffSet + 4, sizeof(unsigned));
                        memcpy(&rid.slotNum, curP + curOffSet + 4 + sizeof(unsigned), sizeof(short));
                        if(curIndex==curSlotNum-1){
                            int nextPageNum;
                            memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                            if(nextPageNum!=-1)
                            {
                                curPageNum=nextPageNum;
                                ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                curOffSet=0;
                                curIndex=0;
                                memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                            }else
                            {
                                curPageNum=-2; // end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }else
                        {
                            curIndex+=1;
                            curOffSet=curOffSet+4+sizeof(unsigned)+sizeof(short);
                        }
                        return 0;
                    }else if(attribute.type==TypeReal){
                        float keyFloatvalue;
                        memcpy(&keyFloatvalue,  curP+curOffSet, 4);
                        memcpy((char *) key,  &keyFloatvalue, 4);
                        memcpy(&rid.pageNum, curP + curOffSet + 4, sizeof(unsigned));
                        memcpy(&rid.slotNum, curP + curOffSet + 4 + sizeof(unsigned), sizeof(short));
                        if(curIndex==curSlotNum-1){
                            int nextPageNum;
                            memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                            if(nextPageNum!=-1)
                            {
                                curPageNum=nextPageNum;
                                ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                curOffSet=0;
                                curIndex=0;
                                memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                            }else
                            {
                                curPageNum=-2; // end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }else
                        {
                            curIndex+=1;
                            curOffSet=curOffSet+4+sizeof(unsigned)+sizeof(short);
                        }
                        return 0;
                    }else{ //attribute==varchar
                        int varCharLength;
                        memcpy(&varCharLength, curP + curOffSet, 4);

                        memcpy((char *)key, curP+curOffSet, 4);
                        memcpy((char *)key+4, curP+curOffSet+4, varCharLength);
                        memcpy(&rid.pageNum, curP + curOffSet + 4+varCharLength, sizeof(unsigned));
                        memcpy(&rid.slotNum, curP + curOffSet + 4 +varCharLength+ sizeof(unsigned), sizeof(short));
                        if(curIndex==curSlotNum-1){
                            int nextPageNum;
                            memcpy(&nextPageNum,curP+PAGE_SIZE-10,4);
                            if(nextPageNum!=-1)
                            {
                                curPageNum=nextPageNum;
                                ixFileHandle->fileHandle.readPage(nextPageNum, curPageBuffer);
                                curOffSet=0;
                                curIndex=0;
                                memcpy(&curSlotNum,(char *)curPageBuffer+PAGE_SIZE-4,2);
                            }else
                            {
                                curPageNum=-2; // end while loop
                                curOffSet=0;
                                curIndex=0;
                            }
                        }else
                        {
                            curIndex+=1;
                            curOffSet=curOffSet+4+varCharLength+sizeof(unsigned)+sizeof(short);
                        }
                        return 0;
                    }
                }
            }
        }
        return IX_EOF;
    }

    RC IX_ScanIterator::close() {
        free(curPageBuffer);
        return 0;
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
        ixReadPageCounter = readPageCount;
        ixWritePageCounter = writePageCount;
        ixAppendPageCounter = appendPageCount;
        return 0;
    }

} // namespace PeterDB