#include "src/include/rbfm.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager()
    {
        PagedFileManager& pfm = PagedFileManager::instance();
        this->pfm = &pfm;
    }

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return pfm->createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return pfm->destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return pfm->openFile(fileName,fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return pfm->closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {

        short recordSize = 0;

        int fields = recordDescriptor.size();  // get fields
        int nullBytes = ceil((double )fields / 8);   // null bytes
        char * nullP=( char *) malloc(nullBytes);
        memcpy(nullP,data,nullBytes);
        bool isNull;

        char * fieldOffsetArray=(char *) malloc(fields*2);
        recordSize+=nullBytes;
        recordSize+=2*fields;
        recordSize+=4;  //  |num of nullbytes|    | num of attr|
        for (int i = 0; i < fields; i++)   // iterate all attributes
        {
            int bytePosition = i / 8;
            int bitPosition = i % 8;
            char b = nullP[bytePosition];
            isNull=  ((b >> (7 - bitPosition)) & 0x1);
            if (isNull)
            {
                memcpy(fieldOffsetArray+i*2,&recordSize,2);
                continue;
            }
            if (recordDescriptor.at(i).type == TypeInt) {
                memcpy(fieldOffsetArray+i*2,&recordSize,2);
                recordSize += 4;
            }
            if (recordDescriptor.at(i).type == TypeReal) {
                memcpy(fieldOffsetArray+i*2,&recordSize,2);
                recordSize += 4;

            }
            if (recordDescriptor.at(i).type == TypeVarChar) {
                memcpy(fieldOffsetArray+i*2,&recordSize,2);
                int varCharLength;
                memcpy(&varCharLength, (char *) data + recordSize-4-fields*2, 4);
                recordSize += varCharLength;
                recordSize += 4;
                }

        }
        free(nullP);

        int pageNums = fileHandle.getNumberOfPages();
        bool isInsert= false;

        short nullLength= (short) nullBytes;
        short fieldLength=(short) fields;

        if(pageNums==0)   // if no page exist, appand new page first
        {
            void *appendPage = malloc(PAGE_SIZE);
            fileHandle.appendPage(appendPage);    // append a new page to insert
            free(appendPage);
            fileHandle.initializePage(pageNums);
            void *readPage = malloc(PAGE_SIZE);
            fileHandle.readPage(pageNums , readPage);
            char *currentPofPage2 = (char *)readPage;
            short freeBytes=4092;
            short slotNum=0;
            slotNum+=1;
            rid.pageNum = pageNums;
            rid.slotNum = slotNum;
            short recordOffset = PAGE_SIZE - 4 - (slotNum-1) * SLOT_SIZE - freeBytes;                //set recordOffset
            memcpy(currentPofPage2 + PAGE_SIZE - 4 - SLOT_SIZE * slotNum , &recordOffset, 2);
            memcpy(currentPofPage2 + PAGE_SIZE - 4 - SLOT_SIZE * slotNum + 2, &recordSize, 2); // set record size
            freeBytes = freeBytes - recordSize - SLOT_SIZE;   // update freebytes
            memcpy(currentPofPage2 + PAGE_SIZE - 2, &freeBytes, 2);
            memcpy(currentPofPage2 + PAGE_SIZE - 4, &slotNum, 2);
            // ***************
            memcpy(currentPofPage2 + recordOffset , &nullLength ,2);  // insert num of nullbytes
            memcpy(currentPofPage2+recordOffset+2,&fieldLength,2);
            for(int i =0;i<fields;i++)
            {
                memcpy(currentPofPage2+recordOffset+4+i*2,fieldOffsetArray+i*2 ,2);
            }
            memcpy(currentPofPage2 + recordOffset+4+fields*2, data, recordSize-4-2*fields);           // insert record into page
            //********

            fileHandle.writePage(pageNums, readPage);       // write page to disk
            free(readPage);
            isInsert= true;
        }

        // if page num >0 check the last page first.
        if(!isInsert) {
            void *tempPage = malloc(PAGE_SIZE);
            fileHandle.readPage(pageNums - 1, tempPage);
            char *currentPofPage = (char *) tempPage;
            short freeBytes;
            short slotNum;
            memcpy(&freeBytes, currentPofPage + PAGE_SIZE - 2, 2);
            memcpy(&slotNum, currentPofPage + PAGE_SIZE - 4, 2);
            if (freeBytes >= (recordSize + SLOT_SIZE)) {
                rid.pageNum = pageNums - 1;
                bool hasEmptySlot = false;
                short insertSlotNum = slotNum + 1;
                for (int i = 1; i <= slotNum; i++) {
                    short offsetBefore;
                    memcpy(&offsetBefore, currentPofPage + PAGE_SIZE - 4 - i * SLOT_SIZE, 2);
                    if (offsetBefore == -1) {
                        hasEmptySlot = true;
                        insertSlotNum = i;
                    }
                }
                short recordOffset = PAGE_SIZE - 4 - slotNum * SLOT_SIZE - freeBytes;                //set record offset

                memcpy(currentPofPage + PAGE_SIZE - 4 - SLOT_SIZE * insertSlotNum, &recordOffset, 2);
                memcpy(currentPofPage + PAGE_SIZE - 4 - SLOT_SIZE * insertSlotNum + 2, &recordSize,
                       2); // set record size
                freeBytes = freeBytes - recordSize - SLOT_SIZE;
                memcpy(currentPofPage + PAGE_SIZE - 2, &freeBytes, 2);
                if (!hasEmptySlot) {
                    slotNum = slotNum + 1;
                }
                memcpy(currentPofPage + PAGE_SIZE - 4, &slotNum, 2);
                rid.slotNum = insertSlotNum;

                // ***************
                memcpy(currentPofPage + recordOffset, &nullLength, 2);  // insert num of nullbytes
                memcpy(currentPofPage + recordOffset + 2, &fieldLength, 2);
                for (int i = 0; i < fields; i++) {
                    memcpy(currentPofPage + recordOffset + 4 + i * 2, fieldOffsetArray + i * 2, 2);
                }
                memcpy(currentPofPage + recordOffset + 4 + fields * 2, data,
                       recordSize - 4 - 2 * fields);           // insert record into page
                //********
                fileHandle.writePage(pageNums - 1, tempPage);       // write page to disk
                isInsert = true;
            }
            free(tempPage);
        }
        // iterate all exist pages
        if(!isInsert) {
            for (int i = 0; i < pageNums - 1; i++) {
                void *tempPage2 = malloc(PAGE_SIZE);
                fileHandle.readPage(i, tempPage2);
                char *currentPofPage = (char *) tempPage2;
                short freeBytes;
                short slotNum;
                memcpy(&freeBytes, currentPofPage + PAGE_SIZE - 2, 2);
                memcpy(&slotNum, currentPofPage + PAGE_SIZE - 4, 2);
                if (freeBytes >= (recordSize + SLOT_SIZE))    // check enough free space
                {
                    rid.pageNum = i;
                    bool hasEmptySlot = false;
                    short insertSlotNum = slotNum + 1;
                    for (int i = 1; i <= slotNum; i++) {
                        short offsetBefore;
                        memcpy(&offsetBefore, currentPofPage + PAGE_SIZE - 4 - i * SLOT_SIZE, 2);
                        if (offsetBefore == -1) {
                            hasEmptySlot = true;
                            insertSlotNum = i;
                        }
                    }
                    short recordOffset =
                            PAGE_SIZE - 4 - slotNum * SLOT_SIZE - freeBytes;                //set record offset
                    memcpy(currentPofPage + PAGE_SIZE - 4 - SLOT_SIZE * insertSlotNum, &recordOffset, 2);
                    memcpy(currentPofPage + PAGE_SIZE - 4 - SLOT_SIZE * insertSlotNum + 2, &recordSize,
                           2); // set record size
                    freeBytes = freeBytes - recordSize - SLOT_SIZE;
                    memcpy(currentPofPage + PAGE_SIZE - 2, &freeBytes, 2);
                    if (!hasEmptySlot) {
                        slotNum = slotNum + 1;
                    }
                    memcpy(currentPofPage + PAGE_SIZE - 4, &slotNum, 2);
                    rid.slotNum = insertSlotNum;

                    // ***************
                    memcpy(currentPofPage + recordOffset, &nullLength, 2);  // insert num of nullbytes
                    memcpy(currentPofPage + recordOffset + 2, &fieldLength, 2);
                    for (int i = 0; i < fields; i++) {
                        memcpy(currentPofPage + recordOffset + 4 + i * 2, fieldOffsetArray + i * 2, 2);
                    }
                    memcpy(currentPofPage + recordOffset + 4 + fields * 2, data,
                           recordSize - 4 - 2 * fields);           // insert record into page
                    //********
                    fileHandle.writePage(i, tempPage2);       // write page to disk
                    isInsert = true;
                    free(tempPage2);
                    break;
                }
                free(tempPage2);
            }
        }

            // no enough space in existed pages, we need to append a new page
        if(isInsert== false) {
                void* appendPage = malloc(PAGE_SIZE);
                fileHandle.appendPage(appendPage);    // append a new page to insert
                free(appendPage);
                fileHandle.initializePage(pageNums);
                void *readPage = malloc(PAGE_SIZE);
                fileHandle.readPage(pageNums, readPage);
                char *currentPofPage2 = (char *) readPage;
                short freeBytes;
                short slotNum=0;
                memcpy(&freeBytes, currentPofPage2 + PAGE_SIZE - 2, 2);
                slotNum = slotNum + 1;
                rid.pageNum = pageNums;
                rid.slotNum = (unsigned short )slotNum;
                short recordOffset = 0;                //set recordOffset

                memcpy( currentPofPage2 + PAGE_SIZE - 4 - SLOT_SIZE * slotNum, &recordOffset, 2);
                memcpy( currentPofPage2 + PAGE_SIZE - 4 - SLOT_SIZE * slotNum + 2, &recordSize,2); // set record size

                freeBytes = freeBytes - recordSize - SLOT_SIZE;   // update freebytes
                memcpy(currentPofPage2 + PAGE_SIZE - 2, &freeBytes, 2);
                memcpy( currentPofPage2 + PAGE_SIZE - 4, &slotNum, 2);
                // ***************
                memcpy(currentPofPage2 + recordOffset , &nullLength ,2);  // insert num of nullbytes
                memcpy(currentPofPage2+recordOffset+2,&fieldLength,2);
                for(int i =0;i<fields;i++)
                {
                    memcpy(currentPofPage2+recordOffset+4+i*2,fieldOffsetArray+2*i ,2);
                }
                memcpy(currentPofPage2 + recordOffset+4+fields*2, data, recordSize-4-2*fields);           // insert record into page
                //********

                fileHandle.writePage(pageNums, readPage);       // write page to disk
                free(readPage);
            }
        free(fieldOffsetArray);
        return 0;


        }



        RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                              const RID &rid, void *data) {

            unsigned pageID=rid.pageNum;
            short slotID=rid.slotNum;


            void *Page=malloc(PAGE_SIZE);
            fileHandle.readPage(pageID,Page);

            char * curPage=(char *)Page;
            short recordOffset;
            memcpy(&recordOffset,curPage+PAGE_SIZE-4-slotID*SLOT_SIZE,2);
            if(recordOffset==-1)
            {
                return -1;
            }
            if(recordOffset<=-4)
            {
                short migratePageNum;
                short migrateSlotNum;
                memcpy(&migratePageNum,curPage+PAGE_SIZE-4-slotID*SLOT_SIZE,2);
                memcpy(&migrateSlotNum,curPage+PAGE_SIZE-4-slotID*SLOT_SIZE+2,2);
                RID migrateRID;
                migrateRID.pageNum=(-1)*(migratePageNum+5);
                migrateRID.slotNum=(-1)*(migrateSlotNum+5);

                return readRecord(fileHandle,recordDescriptor,migrateRID,data);

            }
            short recordSize;
            memcpy(&recordSize,curPage+PAGE_SIZE-4-slotID*SLOT_SIZE+2,2);
            short numOffields;
            memcpy(&numOffields,curPage+recordOffset+2,2);

            memcpy(data,curPage+recordOffset+4+numOffields*2,recordSize-4-numOffields*2);
            free(Page);
            return 0;
        }

        RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                                const RID &rid) {
            void * deletePage= malloc(PAGE_SIZE);
            int rc=fileHandle.readPage(rid.pageNum,deletePage);
            if(rc==-1){
                free(deletePage);
                return -1;}
            char * curPofPage= (char *) deletePage;
            short slotNumBeforeDelete;
            memcpy(&slotNumBeforeDelete,curPofPage+PAGE_SIZE-4,2);
            if(rid.slotNum>slotNumBeforeDelete)
            {
                return -1;
            }

            short freebytesBeforeDelete;
            memcpy(&freebytesBeforeDelete,curPofPage+PAGE_SIZE-2,2);
            short recordOffsetToDelete;
            memcpy(&recordOffsetToDelete,curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE,2);

            if(recordOffsetToDelete==-1)
            {
                return -1;
            }
            if(recordOffsetToDelete<=-4)
            {
                short migratePageNum;
                short migrateSlotNum;
                memcpy(&migratePageNum,curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE,2);
                memcpy(&migrateSlotNum,curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE+2,2);
                RID migrateRID;
                migrateRID.pageNum=(-1)*(migratePageNum+5);
                migrateRID.slotNum=(-1)*(migrateSlotNum+5);

                return deleteRecord(fileHandle,recordDescriptor,migrateRID);
            }

            short recordSizeToDelete;
            memcpy(&recordSizeToDelete,curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE+2,2);

            //delete and migrate ***
            int recordAfterSize=PAGE_SIZE-recordSizeToDelete-recordOffsetToDelete-freebytesBeforeDelete-4-slotNumBeforeDelete*4;
            memcpy( curPofPage+recordOffsetToDelete,curPofPage+recordOffsetToDelete+recordSizeToDelete,recordAfterSize); // delete and migrate
            memset(curPofPage+recordOffsetToDelete+recordAfterSize,0,recordSizeToDelete);

            if(rid.slotNum<slotNumBeforeDelete) {
                for (short i = rid.slotNum + 1; i <= slotNumBeforeDelete; i++) {
                    short recordOffsetAfter;
                    memcpy(&recordOffsetAfter,curPofPage + PAGE_SIZE - 4 - i*SLOT_SIZE,2);
                    if(recordOffsetAfter>=0)
                    {
                        short recordOffsetAfterMigrate=recordOffsetAfter-recordSizeToDelete;
                        memcpy(curPofPage + PAGE_SIZE - 4 - i*SLOT_SIZE,&recordOffsetAfterMigrate,2);
                    }
                }
            }
            short deleteSlotOffset=-1;
            memcpy(curPofPage+PAGE_SIZE-4-SLOT_SIZE*rid.slotNum,&deleteSlotOffset,2);  // set the deleted slot offset  to -1
            short deleteSlotSize=0;
            memcpy(curPofPage+PAGE_SIZE-4-SLOT_SIZE*rid.slotNum+2,&deleteSlotSize,2);  // set the slot  size to 0

            freebytesBeforeDelete=freebytesBeforeDelete+recordSizeToDelete;
            memcpy(curPofPage+PAGE_SIZE-2,&freebytesBeforeDelete,2); //update freeBytes

            fileHandle.writePage(rid.pageNum,deletePage);
            free(deletePage);
            return 0;
        }

        RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,std::ostream &out) {


            int fields=recordDescriptor.size();  // get fields

            int nullBytes = ceil((double )fields / 8);   // null bytes

            char * nullP=( char *) malloc(nullBytes);
            memcpy(nullP,data,nullBytes);

            bool isNull;
            int length;

            int offSet=0;
            offSet+=nullBytes;
            char varChar;
            for(unsigned i=0; i<fields;i++) {
                out<<recordDescriptor.at(i).name<<": ";

                int bytePosition = i / 8;
                int bitPosition = i % 8;
                char b = nullP[bytePosition];
                isNull=  ((b >> (7 - bitPosition)) & 0x1);
                if (isNull)
                {
                    out<<"NULL";
                    if(i!=fields-1)
                    {
                        out<<", ";
                    }
                    continue;
                }

                switch(recordDescriptor.at(i).type) {
                    case TypeInt:

                        int intAttr;
                        memcpy(&intAttr, ( char *) data + offSet, 4);
                        out << intAttr;
                        offSet += 4;
                        break;

                    case TypeReal:
                        float floatAttr;
                        memcpy(&floatAttr, (char *) data + offSet, 4);
                        out << floatAttr;
                        offSet += 4;
                        break;

                    case TypeVarChar:

                        memcpy(&length, (char *)data+offSet, 4);
                        offSet += sizeof(int);
                        for(int i=0;i<length;i++ ) {
                            memcpy(&varChar, (char *) data + offSet+i, 1);
                            out << varChar;
                        }
                        offSet += length;
                        break;
                    }
                if(i!=fields-1)
                {
                    out<<", ";
                }
            }
            out << std::endl;
            free(nullP);
            return 0;


        }



        RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                                const void *data, const RID &rid) {
            short recordSize = 0;

            int fields = recordDescriptor.size();  // get fields
            int nullBytes = ceil((double )fields / 8);   // null bytes
            char * nullP=( char *) malloc(nullBytes);
            memcpy(nullP,data,nullBytes);
            bool isNull;

            char * fieldOffsetArray=(char *) malloc(fields*2);
            recordSize+=nullBytes;
            recordSize+=2*fields;
            recordSize+=4;  //  |num of nullbytes|    | num of attr|
            for (int i = 0; i < fields; i++)   // iterate all attributes
            {
                int bytePosition = i / 8;
                int bitPosition = i % 8;
                char b = nullP[bytePosition];
                isNull=  ((b >> (7 - bitPosition)) & 0x1);
                if (isNull)
                {
                    memcpy(fieldOffsetArray+i*2,&recordSize,2);
                    continue;
                }
                if (recordDescriptor.at(i).type == TypeInt) {
                    memcpy(fieldOffsetArray+i*2,&recordSize,2);
                    recordSize += 4;
                }
                if (recordDescriptor.at(i).type == TypeReal) {
                    memcpy(fieldOffsetArray+i*2,&recordSize,2);
                    recordSize += 4;

                }
                if (recordDescriptor.at(i).type == TypeVarChar) {
                    memcpy(fieldOffsetArray+i*2,&recordSize,2);
                    int varCharLength;
                    memcpy(&varCharLength, (char *) data + recordSize-4-fields*2, 4);
                    recordSize += varCharLength;
                    recordSize += 4;
                }
            }
            free(nullP);
            short nullLength= (short) nullBytes;
            short fieldLength=(short) fields;

            void * updatePage= malloc(PAGE_SIZE);
            fileHandle.readPage(rid.pageNum,updatePage);
            char * curPofPage = (char *)updatePage;
            short recordSizeFormal;
            memcpy(&recordSizeFormal,curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE+2,2);
            short recordOffsetFormal;
            memcpy(&recordOffsetFormal,curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE,2);

            if(recordOffsetFormal==-1)
            {
                return -1;
            }
            if(recordOffsetFormal<=-4)
            {
                short migratePageNum1;
                short migrateSlotNum1;
                memcpy(&migratePageNum1,curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE,2);
                memcpy(&migrateSlotNum1,curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE+2,2);
                RID migrateRID1;
                migrateRID1.pageNum=(-1)*(migratePageNum1+5);
                migrateRID1.slotNum=(-1)*(migrateSlotNum1+5);

                return updateRecord(fileHandle,recordDescriptor,data,migrateRID1);
            }

            short freeBytes;
            memcpy(&freeBytes,curPofPage+PAGE_SIZE-2,2);
            short slotNum;
            memcpy(&slotNum,curPofPage+PAGE_SIZE-4,2);

            if(recordSize > (freeBytes+recordSizeFormal))// should migrate the record
            {
                deleteRecord(fileHandle,recordDescriptor,rid);
                short migratePage;
                short migrateSlot;
                RID newRid;
                insertRecord(fileHandle,recordDescriptor,data,newRid);
                migratePage=(-1)*newRid.pageNum-5;              //  offsett : store the new pagenum*-1 -5
                migrateSlot=(-1)*newRid.slotNum-5;               //  recordsize : store the new slotnum*-1 -5
                memcpy(curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE,&migratePage,2);
                memcpy(curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE+2,&migrateSlot,2);
            }else
            {
                if(recordSizeFormal>=recordSize)  // compact the record
                {
                    memset(curPofPage+recordOffsetFormal,0,recordSizeFormal);  // clean the formal record
                    short sizeAfter=PAGE_SIZE-recordOffsetFormal-recordSizeFormal-freeBytes-4-slotNum*SLOT_SIZE;
                    memcpy(curPofPage+recordOffsetFormal+recordSize,curPofPage+recordOffsetFormal+recordSizeFormal,sizeAfter);
                    freeBytes=freeBytes+recordSizeFormal-recordSize;
                    memset(curPofPage+recordOffsetFormal+recordSize+sizeAfter,0,recordSizeFormal-recordSize);

                    memcpy(curPofPage+PAGE_SIZE-2,&freeBytes,2);   //reset freebytes
                    memcpy(curPofPage + recordOffsetFormal , &nullLength ,2);  // insert num of nullbytes
                    memcpy(curPofPage+recordOffsetFormal+2,&fieldLength,2);
                    for(int i =0;i<fields;i++)
                    {
                        memcpy(curPofPage + recordOffsetFormal+4+i*2,fieldOffsetArray+i*2 ,2);
                    }
                    memcpy(curPofPage + recordOffsetFormal+4+fields*2, data, recordSize-4-2*fields);
                    memcpy(curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE+2,&recordSize,2); //reset record size

                    // reset offset of records after that
                    if(rid.slotNum<slotNum) {
                        for (short i = rid.slotNum + 1; i <= slotNum; i++) {
                            short recordOffsetAfter;
                            memcpy(&recordOffsetAfter, curPofPage + PAGE_SIZE - 4 - i * SLOT_SIZE, 2);
                            if (recordOffsetAfter >=0) {
                                short recordOffsetAfterMigrate = recordOffsetAfter - (recordSizeFormal - recordSize);
                                memcpy(curPofPage + PAGE_SIZE - 4 - i * SLOT_SIZE, &recordOffsetAfterMigrate, 2);
                            }
                        }
                    }

                }else        // move the record after that
                {
                    memset(curPofPage+recordOffsetFormal,0,recordSizeFormal);  // clean the formal record
                    short sizeAfter=PAGE_SIZE-recordOffsetFormal-recordSizeFormal-freeBytes-4-slotNum*SLOT_SIZE;
                    memcpy(curPofPage+recordOffsetFormal+recordSize,curPofPage+recordOffsetFormal+recordSizeFormal,sizeAfter);
                    freeBytes=freeBytes-recordSize+recordSizeFormal;
                    memset(curPofPage+recordOffsetFormal+recordSizeFormal,0,recordSize-recordSizeFormal);

                    memcpy(curPofPage+PAGE_SIZE-2,&freeBytes,2);   //reset freebytes
                    memcpy(curPofPage + recordOffsetFormal , &nullLength ,2);  // insert num of nullbytes
                    memcpy(curPofPage+recordOffsetFormal+2,&fieldLength,2);
                    for(int i =0;i<fields;i++)
                    {
                        memcpy(curPofPage + recordOffsetFormal+4+i*2,fieldOffsetArray+i*2 ,2);
                    }
                    memcpy(curPofPage + recordOffsetFormal+4+fields*2, data, recordSize-4-2*fields);  // update data
                    memcpy(curPofPage+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE+2,&recordSize,2); //reset record size

                    // reset offset of records after that
                    if(rid.slotNum<slotNum) {
                        for (short i = rid.slotNum + 1; i <= slotNum; i++) {
                            short recordOffsetAfter;
                            memcpy(&recordOffsetAfter, curPofPage + PAGE_SIZE - 4 - i * SLOT_SIZE, 2);
                            if (recordOffsetAfter >=0) {
                                short recordOffsetAfterMigrate = recordOffsetAfter + (recordSize-recordSizeFormal);
                                memcpy(curPofPage + PAGE_SIZE - 4 - i * SLOT_SIZE, &recordOffsetAfterMigrate, 2);
                            }
                        }
                    }
                }
            }
            fileHandle.writePage(rid.pageNum,updatePage);
            free(updatePage);
            free(fieldOffsetArray);
            return 0;
        }

        RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                                 const RID &rid, const std::string &attributeName, void *data) {
            void * Page=malloc(PAGE_SIZE);
            fileHandle.readPage(rid.pageNum,Page);
            char  * curP=(char *)Page;
            short recordSize;
            short recordOffset;
            memcpy(&recordOffset,curP+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE,2);
            memcpy(&recordSize,curP+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE+2,2);
            if(recordOffset==-1)
            {
                return -1;
            }
            if(recordOffset<=-4)
            {
                short migratePageNum;
                short migrateSlotNum;
                memcpy(&migratePageNum,curP+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE,2);
                memcpy(&migrateSlotNum,curP+PAGE_SIZE-4-rid.slotNum*SLOT_SIZE+2,2);
                RID migrateRID;
                migrateRID.pageNum=(-1)*(migratePageNum+5);
                migrateRID.slotNum=(-1)*(migrateSlotNum+5);

                return readAttribute(fileHandle,recordDescriptor,migrateRID,attributeName,data);

            }

            int fields = recordDescriptor.size();  // get fields

            short nullBytes;
            memcpy(&nullBytes,curP+recordOffset,2);
            char * nullP=( char *) malloc(nullBytes);
            memcpy(nullP,curP+recordOffset+4+fields*2,nullBytes);
            bool isNull;

            char * pData= (char *)data;

            for (int i =0;i<fields;i++)
            {
                int bytePosition = i / 8;
                int bitPosition = i % 8;
                char b = nullP[bytePosition];
                isNull=  ((b >> (7 - bitPosition)) & 0x1);
                if(recordDescriptor.at(i).name==attributeName)
                {
                    auto * nullIndicator = (unsigned char*)malloc(1);
                    memset(nullIndicator,0, 1);
                    if(!isNull)
                    {
                        //short checkNull=0;
                        //memcpy(pData,&checkNull,2);
                        memcpy(pData,nullIndicator,1);
                        if(recordDescriptor.at(i).type==TypeInt||recordDescriptor.at(i).type==TypeReal) {
                            short attrOffset;
                            memcpy(&attrOffset,curP+recordOffset+4+i*2,2);
                            memcpy(pData+1,curP+recordOffset+attrOffset,4);

                        }
                        if(recordDescriptor.at(i).type==TypeVarChar) {
                            short attrOffset;
                            memcpy(&attrOffset,curP+recordOffset+4+i*2,2);
                            short varcharLength;
                            if(i<fields-1) {
                                short attOffsetOfNext;
                                memcpy(&attOffsetOfNext, curP+recordOffset + 4  + (i + 1) * 2, 2);
                                varcharLength=attOffsetOfNext-attrOffset-4;
                            }else{
                                varcharLength=recordSize-attrOffset-4;
                            }
                            int vcLen=(int) varcharLength;
                            memcpy(pData+1,&vcLen,4);
                            memcpy(pData+5,curP+recordOffset+attrOffset+4,varcharLength);
                        }
                    }
                    else// is null
                    {
                        //short checkNull=-1;
                        //memcpy(pData,&checkNull,2);
                        int byteIndex = 0/ 8;
                        int bitIndex = 0 % 8;
                        nullIndicator[byteIndex] += pow(2, 7-bitIndex);
                        memcpy(pData,nullIndicator,1);
                    }
                    free(nullIndicator);
                    free(nullP);
                    free(Page);
                    return 0;
                }
            }
            free(nullP);
            free(Page);
            return -1;
        }

        RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                        const std::vector<std::string> &attributeNames,
                                        RBFM_ScanIterator &rbfm_ScanIterator) {
            rbfm_ScanIterator.init(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames, this);
            return 0;
        }

        // RBFM_ScanIterator FUNCTION***************
        RC RBFM_ScanIterator::init(FileHandle &fileHandle,const std::vector<Attribute> &recordDescriptor,const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                   const std::vector<std::string> &attributeNames, RecordBasedFileManager *rbfm)
       {
            this->fileHandle=fileHandle;
           this->recordDescriptor=recordDescriptor;
           this->conditionAttrName=conditionAttribute;
           this->attributeNames=attributeNames;
           this->compOp=compOp;
           this->tempRid.slotNum=1;
           this->tempRid.pageNum=0;
           this->value=value;
           this->rbfm= rbfm;

           for(int i=0;i<recordDescriptor.size();i++)
           {
               if(recordDescriptor.at(i).name==conditionAttribute)
               {
                   this->conditionAttrType=recordDescriptor.at(i).type;
                   this->conditionAttrIndex=i;
               }
           }

           for(int j=0;j<attributeNames.size();j++)
           {
               for (int i=0;i<recordDescriptor.size();i++)
               {
                   if(recordDescriptor.at(i).name==attributeNames[j])
                   {
                       this->retrieveAttrIndex.push_back(i);
                       break;
                   }
               }
           }
           if(this->retrieveAttrIndex.empty()){return -1;}

           return 0;
       }


        RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
        {

            int totalPageNums=fileHandle.getNumberOfPages();


            char * PofCompareValue=(char *)value;

            while(tempRid.pageNum<totalPageNums) // loop until find the first record
            {
                void * pageData = malloc(PAGE_SIZE);
                RC rc= fileHandle.readPage(tempRid.pageNum,pageData);
                if(rc==-1){return RBFM_EOF;}
                char * curP= (char *) pageData;

                short totalSlotNum;
                memcpy(&totalSlotNum,curP+PAGE_SIZE-4,2);
                if(tempRid.slotNum<=totalSlotNum)
                {
                    bool no_opCheck=false;
                    if(conditionAttrName=="" && compOp==NO_OP){
                        no_opCheck= true;
                    }
                    void * conditionAttrData= malloc(100);

                    if(conditionAttrName!="") {
                        rc=rbfm->readAttribute(fileHandle, recordDescriptor, tempRid, conditionAttrName,conditionAttrData);
                    }
                    char * PofConAttrData=(char *) conditionAttrData;

                    short recordOffset;
                    memcpy(&recordOffset,curP+PAGE_SIZE-4-tempRid.slotNum*SLOT_SIZE,2);
                    if(recordOffset<0)
                    {
                        rc=-1;
                        no_opCheck= false;
                    }

                    if(rc!=-1||no_opCheck== true)  // cotain the contionAttr
                    {
                        bool isSatisfy=false;
                        //short nullCheck;
                        //memcpy(&nullCheck,PofConAttrData,2);
                        char * nullP=( char *) malloc(1);
                        memcpy(nullP,PofConAttrData,1);
                        bool isNull;
                        int bytePosition = 0 / 8;
                        int bitPosition = 0 % 8;
                        char b = nullP[bytePosition];
                        isNull=  ((b >> (7 - bitPosition)) & 0x1);

                        if(compOp==NO_OP)// no option
                        {
                            isSatisfy= true;
                        }
                        if(isNull) // is  NULL
                        {
                            if(PofCompareValue== nullptr && compOp==EQ_OP)
                            {
                                isSatisfy= true;
                            }
                        }
                        else if(conditionAttrName!="")
                        { // not null
                            if(conditionAttrType==TypeInt)
                            {
                                int attrIntValue;
                                memcpy(&attrIntValue,PofConAttrData+1,4);
                                int compValue;
                                memcpy(&compValue,PofCompareValue,4);

                                switch (compOp) {
                                    case EQ_OP:
                                        isSatisfy=(attrIntValue==compValue);
                                        break;
                                    case LT_OP:
                                        isSatisfy=(attrIntValue<compValue);
                                        break;
                                    case LE_OP:
                                        isSatisfy=(attrIntValue<=compValue);
                                        break;
                                    case GT_OP:
                                        isSatisfy=(attrIntValue>compValue);
                                        break;
                                    case GE_OP:
                                        isSatisfy=(attrIntValue>=compValue);
                                        break;
                                    case NE_OP:
                                        isSatisfy=(attrIntValue!=compValue);
                                        break;
                                }
                            }
                            if(conditionAttrType==TypeReal)
                            {
                                float attrFloatValue;
                                memcpy(&attrFloatValue,PofConAttrData+1,4);
                                float compValue;
                                memcpy(&compValue,PofCompareValue,4);

                                switch (compOp) {
                                    case EQ_OP:
                                        isSatisfy=(attrFloatValue==compValue);
                                        break;
                                    case LT_OP:
                                        isSatisfy=(attrFloatValue<compValue);
                                        break;
                                    case LE_OP:
                                        isSatisfy=(attrFloatValue<=compValue);
                                        break;
                                    case GT_OP:
                                        isSatisfy=(attrFloatValue>compValue);
                                        break;
                                    case GE_OP:
                                        isSatisfy=(attrFloatValue>=compValue);
                                        break;
                                    case NE_OP:
                                        isSatisfy=(attrFloatValue!=compValue);
                                        break;
                                }
                            }
                            if(conditionAttrType==TypeVarChar)
                            {
                                int attrVarcharLength;
                                memcpy(&attrVarcharLength,PofConAttrData+1,4);

                                int compVarcharLength;
                                memcpy(&compVarcharLength,PofCompareValue,4);

                                switch (compOp) {
                                    case EQ_OP:
                                        isSatisfy=(string(PofConAttrData+5,attrVarcharLength) ==string(PofCompareValue+4,compVarcharLength));
                                        break;
                                    case LT_OP:
                                        isSatisfy=(string(PofConAttrData+5,attrVarcharLength) < string(PofCompareValue+4,compVarcharLength));
                                        break;
                                    case LE_OP:
                                        isSatisfy=(string(PofConAttrData+5,attrVarcharLength) <=string(PofCompareValue+4,compVarcharLength));
                                        break;
                                    case GT_OP:
                                        isSatisfy=(string(PofConAttrData+5,attrVarcharLength) > string(PofCompareValue+4,compVarcharLength));
                                        break;
                                    case GE_OP:
                                        isSatisfy=(string(PofConAttrData+5,attrVarcharLength) >= string(PofCompareValue+4,compVarcharLength));
                                        break;
                                    case NE_OP:
                                        isSatisfy=(string(PofConAttrData+5,attrVarcharLength) != string(PofCompareValue+4,compVarcharLength));
                                        break;
                                }
                            }
                        }  // not NULL
                        free(nullP);
                        if(isSatisfy== true) // if satisfy , retrieve the data
                        {
                            char * PofRetrieveRD=(char *) data;
                            bool isRetrieved= true;
                            //genereate null indicator first
                            int fields=retrieveAttrIndex.size();
                            int nullBytesLen = ceil((double )fields / 8);   // null bytes
                            auto * nullIndicator = (unsigned char*)malloc(nullBytesLen);
                            memset(nullIndicator,0, nullBytesLen);
                            int recordSize=nullBytesLen;
                            for(int i=0;i<retrieveAttrIndex.size();i++)
                            {
                                int index=retrieveAttrIndex[i];
                                void * attributeData= malloc(200);
                                RC rc=rbfm->readAttribute(fileHandle,recordDescriptor,tempRid,recordDescriptor.at(index).name,attributeData);
                                if(rc==-1)
                                {
                                    isRetrieved = false;
                                    free(attributeData);
                                    break;
                                }
                                char * PofAttributeData=(char *)attributeData;

                                //short nullCheck;
                                //memcpy(&nullCheck,PofAttributeData,2);
                                char * nullP2=( char *) malloc(1);
                                memcpy(nullP2,PofAttributeData,1);
                                bool isNull2=false;
                                int bytePosition2 = 0;
                                int bitPosition2 = 0;
                                char b2 = nullP2[bytePosition2];
                                isNull2=  ((b2 >> (7 - bitPosition2)) & 0x1);

                                if(isNull2) //is null
                                {
                                    int byteIndex = i / 8;
                                    int bitIndex = i % 8;
                                    nullIndicator[byteIndex] += pow(2, 7-bitIndex);
                                }else {

                                    if (recordDescriptor.at(index).type == TypeInt) {
                                        memcpy(PofRetrieveRD+recordSize,PofAttributeData+1,4);
                                        recordSize+=4;
                                    }
                                    if (recordDescriptor.at(index).type == TypeReal)
                                    {
                                        memcpy(PofRetrieveRD+recordSize,PofAttributeData+1,4);
                                        recordSize+=4;
                                    }
                                    if (recordDescriptor.at(index).type == TypeVarChar)
                                    {
                                        int vcLen;
                                        memcpy(&vcLen,PofAttributeData+1,4);
                                        memcpy(PofRetrieveRD+recordSize,PofAttributeData+1,4);
                                        recordSize+=4;
                                        memcpy(PofRetrieveRD+recordSize,PofAttributeData+5,vcLen);
                                        recordSize+=vcLen;
                                    }
                                }
                                free(attributeData);
                                free(nullP2);
                            }
                            if(isRetrieved){
                                memcpy(PofRetrieveRD,nullIndicator,nullBytesLen);
                                free(nullIndicator);
                                rid.slotNum=tempRid.slotNum;
                                rid.pageNum=tempRid.pageNum;
                                tempRid.slotNum+=1;
                                free(conditionAttrData);
                                free(pageData);
                                return 0;
                            }
                        }// retrieve end

                    }// if not contain the attribute or not satisfy free the memory and continue the for loop
                    free(conditionAttrData);
                    tempRid.slotNum+=1;
                }
                if(tempRid.slotNum>totalSlotNum){
                    tempRid.pageNum+=1;
                    tempRid.slotNum=1;
                }

                free(pageData);
            } // end
            return RBFM_EOF;
        }

        RC RBFM_ScanIterator::close()
        {
            retrieveAttrIndex.clear();
            return 0;
        }

} // namespace PeterDB

