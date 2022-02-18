#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        FILE *fp;
        fp= fopen(fileName.c_str(),"rb");
        if(fp)
        {
            return -1;  //file exist
        }
        fp = fopen(fileName.c_str(), "wb");

        unsigned readPageCounter = 0;
        unsigned writePageCounter = 0;
        unsigned appendPageCounter = 0;
        unsigned pageNums = 0;

        void * newPage= malloc(PAGE_SIZE);
        char * cur= (char *)newPage;
        memcpy( cur,&readPageCounter, 4);
        memcpy( cur+4 ,&writePageCounter, 4);
        memcpy( cur+8,&appendPageCounter, 4);
        memcpy( cur+12,&pageNums, 4);

        fwrite(newPage,PAGE_SIZE,1,fp);

        free(newPage);

        fclose(fp);
        if(!fp)
        {
            return -1;
        }
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        FILE *fp;
        fp= fopen(fileName.c_str(),"rb");
        if (!fp)
        {
            return -1;
        }

        fclose(fp);

        remove(fileName.c_str());
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        FILE * FP=fileHandle.getFileP();
        if(FP!= nullptr)
        {
            return -1;
        }
        FILE *fp;
        fp= fopen(fileName.c_str(),"rb");
        if (!fp)
        {
            return -1;
        }
        fclose(fp);
        fp= fopen(fileName.c_str(),"rb+");
        fileHandle.setFileP(fp);
        fileHandle.readHiddenPage();
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        fileHandle.closeFile();
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        pageNums=0;
    }

    FileHandle::~FileHandle()
    {
        if(fileP)
        {fclose(fileP);
        fileP = NULL;}
    };

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (pageNum>getNumberOfPages())
        {
            return -1;
        }
        int seek= fseek(fileP,(pageNum+1)*PAGE_SIZE,SEEK_SET);
        if (seek!=0)
        {
            return -1;     // read size larger than file size
        }

        size_t readCount=fread((char *)data,PAGE_SIZE,1,fileP);
        if(readCount!=1)
        {
            return -1;
        }
        readPageCounter++;
        updateHiddenPage();
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {

        if (pageNum>getNumberOfPages())
        {
            return -1;
        }

        if (fseek(fileP,(pageNum+1)*PAGE_SIZE,SEEK_SET)!=0)
        {
            return -1;
        }

        fwrite((char *)data,PAGE_SIZE,1,fileP);
        writePageCounter++;
        updateHiddenPage();
        return 0;

    }

    RC FileHandle::appendPage(const void *data) {
        if(!fileP){return -1;}
        fseek(fileP,0,SEEK_END);

        fwrite(data,1,PAGE_SIZE,fileP);

        fseek(fileP, 0, SEEK_SET);
        pageNums++;
        appendPageCounter++;
        updateHiddenPage();
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        fseek(fileP, 0, SEEK_END);

        return ftell(fileP)/PAGE_SIZE-1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount=readPageCounter;
        writePageCount=writePageCounter;
        appendPageCount=appendPageCounter;
        return 0;
    }

    void FileHandle::initializePage(PageNum pageNum)
    {
        void * newPage= malloc(PAGE_SIZE);
        short slotNum=0;
        short freeBytes=4092;
        char * cur=(char *)newPage;

        memcpy(cur+PAGE_SIZE-2,&freeBytes,2);   // initialize a page with F and N
        memcpy(cur+PAGE_SIZE-4,&slotNum,2);
        writePage(pageNum,newPage);
        free(newPage);
    }

    void FileHandle::initializeRootPage(PageNum pageNum)
    {
        void * newPage= malloc(PAGE_SIZE);
        short slotNum=1;
        short freeBytes=4092;
        char * cur=(char *)newPage;
        int rootPointer=1;
        memcpy(cur+PAGE_SIZE-2,&freeBytes,2);   // initialize a page with F and N
        memcpy(cur+PAGE_SIZE-4,&slotNum,2);
        memcpy(cur,&rootPointer,4);
        writePage(pageNum,newPage);
        free(newPage);
    }


    void FileHandle::updateHiddenPage()
    {
        fseek(fileP,0,SEEK_SET );
        void * P= malloc(16 );
        char * cur=(char *) P;
        memcpy(     cur,&readPageCounter, 4);
        memcpy( cur+4,&writePageCounter, 4);
        memcpy( cur+8,&appendPageCounter, 4);
        memcpy( cur+12,&pageNums, 4);

        fwrite(cur,16,1,fileP);

        free(P);
    }

    void FileHandle::readHiddenPage()
    {
        fseek(fileP,0,SEEK_SET );
        void * P= malloc(16 );
        char * cur=(char *) P;
        fread(cur,16,1,fileP);
        memcpy(&readPageCounter, cur,4);
        memcpy(&writePageCounter, cur+4,4);
        memcpy(&appendPageCounter, cur+8,4);
        memcpy(&pageNums, cur+12,4);
        free(P);
    }



    RC FileHandle::closeFile()
    {

        if (!fileP)
        {
            return -1;
        }
        fclose(fileP);
        fileP=NULL;
        return 0;
    }

    RC FileHandle::setFileP(FILE* _fileP)
    {
        fileP=_fileP;
        return 0;
    }

    FILE * FileHandle::getFileP()
    {
        return fileP;
    }
} // namespace PeterDB