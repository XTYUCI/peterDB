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
        int nullBytes = ceil(fields / 8)+1;   // null bytes
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

        if(pageNums==121)   // if no page exist, appand new page first
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

        void *tempPage = malloc(PAGE_SIZE);
        fileHandle.readPage(pageNums-1, tempPage);
        char * currentPofPage= (char *) tempPage;
        short freeBytes;
        short slotNum;
        memcpy(&freeBytes, currentPofPage + PAGE_SIZE - 2, 2);
        memcpy(&slotNum, currentPofPage + PAGE_SIZE - 4, 2);
        if (freeBytes >= (recordSize + SLOT_SIZE)) {
            rid.pageNum = pageNums-1;
            slotNum = slotNum + 1;
            rid.slotNum = slotNum;
            short recordOffset = PAGE_SIZE - 4 - (slotNum-1) * SLOT_SIZE - freeBytes;                //set record offset
            memcpy(currentPofPage + PAGE_SIZE - 4 - SLOT_SIZE * slotNum , &recordOffset, 2);
            memcpy(currentPofPage + PAGE_SIZE - 4 - SLOT_SIZE * slotNum  + 2, &recordSize,2); // set record size
            freeBytes = freeBytes - recordSize - SLOT_SIZE;
            memcpy(currentPofPage + PAGE_SIZE - 2, &freeBytes, 2);
            memcpy(currentPofPage + PAGE_SIZE - 4, &slotNum, 2);

            // ***************
            memcpy(currentPofPage + recordOffset , &nullLength ,2);  // insert num of nullbytes
            memcpy(currentPofPage+recordOffset+2,&fieldLength,2);
            for(int i =0;i<fields;i++)
            {
                memcpy(currentPofPage+recordOffset+4+i*2,fieldOffsetArray+i*2 ,2);
            }
            memcpy(currentPofPage + recordOffset+4+fields*2, data, recordSize-4-2*fields);           // insert record into page
            //********
            fileHandle.writePage(pageNums-1, tempPage);       // write page to disk
            isInsert= true;
        }
        free(tempPage);

        // iterate all exist pages

        for (int i = 0; i < pageNums-1; i++)
        {
            void *tempPage2 = malloc(PAGE_SIZE);
            fileHandle.readPage(i, tempPage2);
            char *currentPofPage = (char *) tempPage2;
            short freeBytes;
            short slotNum;
            memcpy(&freeBytes,  currentPofPage + PAGE_SIZE - 2, 2);
            memcpy(&slotNum,  currentPofPage + PAGE_SIZE - 4, 2);
            if (freeBytes >= (recordSize + SLOT_SIZE))    // check enough free space
            {
                rid.pageNum = i;
                slotNum = slotNum + 1;
                rid.slotNum = slotNum;
                short recordOffset =
                        PAGE_SIZE - 4 - (slotNum-1)  * SLOT_SIZE - freeBytes;                //set record offset
                memcpy( currentPofPage + PAGE_SIZE - 4 - SLOT_SIZE * slotNum, &recordOffset, 2);
                memcpy( currentPofPage + PAGE_SIZE - 4 - SLOT_SIZE * slotNum + 2, &recordSize,2); // set record size
                freeBytes = freeBytes - recordSize - SLOT_SIZE;
                memcpy( currentPofPage + PAGE_SIZE - 2, &freeBytes, 2);
                memcpy( currentPofPage + PAGE_SIZE - 4, &slotNum, 2);
                // ***************
                memcpy(currentPofPage + recordOffset , &nullLength ,2);  // insert num of nullbytes
                memcpy(currentPofPage+recordOffset+2,&fieldLength,2);
                for(int i =0;i<fields;i++)
                {
                    memcpy(currentPofPage+recordOffset+4+i*2,fieldOffsetArray+i*2 ,2);
                }
                memcpy(currentPofPage + recordOffset+4+fields*2, data, recordSize-4-2*fields);           // insert record into page
                //********
                fileHandle.writePage(i, tempPage2);       // write page to disk
                isInsert= true;
            }
            free(tempPage2);
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
                memcpy(currentPofPage + recordOffset , &nullLength ,2);  // insert num of nullbytes
                memcpy(currentPofPage+recordOffset+2,&fieldLength,2);
                for(int i =0;i<fields;i++)
                {
                    memcpy(currentPofPage+recordOffset+4+i*2,fieldOffsetArray+2*i ,2);
                }
                memcpy(currentPofPage + recordOffset+4+fields*2, data, recordSize-4-2*fields);           // insert record into page
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
            return -1;
        }

        RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,std::ostream &out) {


            int fields=recordDescriptor.size();  // get fields

            int nullBytes= ceil((fields /8))+1;   // null bytes

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
            return -1;
        }

        RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                                 const RID &rid, const std::string &attributeName, void *data) {
            return -1;
        }

        RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                        const std::vector<std::string> &attributeNames,
                                        RBFM_ScanIterator &rbfm_ScanIterator) {
            return -1;
        }




    } // namespace PeterDB

