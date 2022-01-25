#include "src/include/rm.h"
#include <fstream>

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager()
    {
        initializeTablesDescriptor();
        initializeColumnsDescriptor();
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        this->rbfm = &rbfm;

    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        RC rc=rbfm->createFile("Tables");
        if(rc==-1){return -1;}
        rc=rbfm->createFile("Columns");
        if(rc==-1){return -1;}

        rc = insertRecordToTables(1,"Tables","Tables");
        if(rc==-1){return -1;}
        rc= insertRecordToTables(2,"Columns","Columns");
        if(rc==-1){return -1;}

        rc=insertRecordToColumns(1,tablesRecordDescriptor);
        if(rc==-1){return -1;}
        rc=insertRecordToColumns(2,columnsRecordDescriptor);
        if(rc==-1){return -1;}
        return 0;
    }

    RC RelationManager::deleteCatalog() {

        RC rc=rbfm->destroyFile("Tables");
        if(rc==-1){return -1;}
        rc=rbfm->destroyFile("Columns");
        if(rc==-1){return -1;}
        remove("Tables");
        remove("Columns");
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        if(tableName=="Tables"||tableName=="Columns"){return -1;}

        std::fstream tablesCatalogFile;
        tablesCatalogFile.open("Tables", std::ios::in | std::ios::binary);
        if (tablesCatalogFile.is_open()) tablesCatalogFile.close();
        else return -1;

        // Try open Columns at input mode. If unsuccessful, doesn't exist
        std::fstream columnsCatalogFile;
        columnsCatalogFile.open("Columns", std::ios::in | std::ios::binary);
        if (columnsCatalogFile.is_open()) columnsCatalogFile.close();
        else return -1;

        int maxTablesIndex=getTablesMaxIndex();

        RC rc= insertRecordToTables(maxTablesIndex+1,tableName,tableName);
        if(rc==-1){return -1;}

        rc= insertRecordToColumns(maxTablesIndex+1,attrs);
        if(rc==-1){return -1;}

        rc= rbfm->createFile(tableName);
        if(rc==-1){return -1;}
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if(tableName=="Tables"||tableName=="Columns"){return -1;}

        RID rid;
        int deleteID= getTableId(tableName,rid); // get delete id
        FileHandle tfileHandle;
        RC rc = rbfm ->openFile("Tables",tfileHandle);  // delete record in Tables;
        if(rc==-1){return -1;}
        if(deleteID==-1){return -2;} // tablename not in tables
        rc= rbfm->deleteRecord(tfileHandle,tablesRecordDescriptor,rid);
        if(rc==-1){return -1;}
        rbfm->closeFile(tfileHandle);

        // delete record in Columns
        RM_ScanIterator rm_ScanIterator;
        vector<string> attributeNames;
        attributeNames.emplace_back("Attribute_name");
        void * data= malloc(PAGE_SIZE);

        RID deletedColumnRid;

        scan("Columns","Table_id",EQ_OP,&deleteID,attributeNames,rm_ScanIterator);
        while(rm_ScanIterator.getNextTuple(deletedColumnRid,data)!=RM_EOF)
        {
            rc= rbfm->deleteRecord(rm_ScanIterator.rbfm_scanIterator.fileHandle,columnsRecordDescriptor,deletedColumnRid);
            if(rc==-1){return -1;}
        }
        rm_ScanIterator.close();
        free(data);;

        rc=rbfm->destroyFile(tableName);
        remove(tableName.c_str());
        if(rc==-1){return -1;}
        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {

        if(tableName=="Tables"){attrs=tablesRecordDescriptor;}
        else if(tableName=="Columns"){attrs=columnsRecordDescriptor;}
        else
        {
            RID rid;
            int tableId= getTableId(tableName,rid);
            if (tableId==-1){return -2;}
            vector<string> attributeNames;
            attributeNames.push_back("Attribute_name");
            attributeNames.push_back("Type");
            attributeNames.push_back("Length");


            RM_ScanIterator rmScanIterator;
            scan("Columns","Table_id",EQ_OP,&tableId,attributeNames,rmScanIterator);
            void * data= malloc(PAGE_SIZE);
            char * Pdata=(char *)data;
            while(rmScanIterator.getNextTuple(rid,data)!=RM_EOF)
            {
                Attribute attribute;
                int varcharLength;
                memcpy(&varcharLength,Pdata+1,4);
                char *name = (char *) malloc(varcharLength);
                memcpy(name,Pdata+5,varcharLength);
                attribute.name=string (name,varcharLength);
                free(name);
                memcpy(&attribute.type,Pdata+5+varcharLength,4);
                memcpy(&attribute.length,Pdata+5+varcharLength+4,4);
                attrs.push_back(attribute);
            }
            free(data);
            rmScanIterator.close();

        }
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        FileHandle fileHandle;
        RC rc= rbfm->openFile(tableName,fileHandle);
        if(rc==-1){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        rc=rbfm->insertRecord(fileHandle,recordDescriptor,data,rid);
        if(rc==-1){return -1;}
        rc=rbfm->closeFile(fileHandle);
        if(rc==-1){return -1;}


        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        FileHandle fileHandle;
        RC rc= rbfm->openFile(tableName,fileHandle);
        if(rc==-1){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        rc=rbfm->deleteRecord(fileHandle,recordDescriptor,rid);
        if(rc==-1){return -1;}
        rc=rbfm->closeFile(fileHandle);
        if(rc==-1){return -1;}
        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        FileHandle fileHandle;
        RC rc= rbfm->openFile(tableName,fileHandle);
        if(rc==-1){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        rc=rbfm->updateRecord(fileHandle,recordDescriptor,data,rid);
        if(rc==-1){return -1;}
        rc=rbfm->closeFile(fileHandle);
        if(rc==-1){return -1;}
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        FileHandle fileHandle;
        RC rc= rbfm->openFile(tableName,fileHandle);
        if(rc==-1){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        rc=rbfm->readRecord(fileHandle,recordDescriptor,rid,data);
        if(rc==-1){return -1;}
        rc=rbfm->closeFile(fileHandle);
        if(rc==-1){return -1;}
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        rbfm->printRecord(attrs,data,out);
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        FileHandle fileHandle;
        RC rc= rbfm->openFile(tableName,fileHandle);
        if(rc==-1){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        rc=rbfm->readAttribute(fileHandle,recordDescriptor,rid,attributeName,data);
        if(rc==-1){return -1;}
        rc=rbfm->closeFile(fileHandle);
        if(rc==-1){return -1;}
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);

        RC rc=rbfm->openFile(tableName,rm_ScanIterator.rbfm_scanIterator.fileHandle);
        if(rc==-1){return -1;}

        rbfm->scan(rm_ScanIterator.rbfm_scanIterator.fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfm_scanIterator);

        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
    {
        RC rc=rbfm_scanIterator.getNextRecord(rid,data);
        if(rc==RBFM_EOF){return RM_EOF;}
        return 0;

    }

    RC RM_ScanIterator::close() {
        RC rc=rbfm_scanIterator.close();
        if(rc==-1){return -1;}
        rc=rbfm_scanIterator.fileHandle.closeFile();
        if(rc==-1){return -1;}
        return 0; }





    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }
    //******************************

    int RelationManager::getTablesMaxIndex()  // get max index of Tables table
     {
        vector<string> attributeNames;
        attributeNames.emplace_back("Id");

        RM_ScanIterator rm_scanIterator;
        scan("Tables","",NO_OP, nullptr,attributeNames,rm_scanIterator);
        int maxId=0;
        RID scanRid;
        void * idData= malloc(5); // nullcheck + int size
        while (rm_scanIterator.getNextTuple(scanRid,idData)!=RM_EOF)
        {
            maxId+=1;
        }
        free(idData);
        rm_scanIterator.close();
        return maxId;
    }

    int RelationManager::getTableId(const string &tableName, RID &rid)
    {
        int tableId=-1;
        vector<string> attributeNames;
        attributeNames.emplace_back("Id");

        int nameLength=tableName.length();
        void * value= malloc(4+nameLength);
        char * PofValue=(char *) value;
        memcpy(PofValue,&nameLength,4);
        memcpy(PofValue+4,tableName.c_str(),nameLength);
        RM_ScanIterator rm_ScanIterator;
        RC rc=scan("Tables","Name",EQ_OP,value,attributeNames,rm_ScanIterator);
        if(rc==-1){return -1;}
        void * data= malloc(5);
        char * PofData=(char *) data;
        if(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF)
        {
            memcpy(&tableId,PofData+1,4);
        }
        rm_ScanIterator.close();
        free(data);
        free(value);
        return tableId;
    }

    RC RelationManager::insertRecordToTables(int ID, const string &tableName, const string &fileName)
    {
        FileHandle fileHandle;
        RC rc= rbfm->openFile("Tables",fileHandle);
        if(rc==-1){return -1;}
        void * data= malloc(1024);
        char * PofData=(char *) data;
        int dataSize=0;
        char nullIndicator=0;
        memcpy(PofData,&nullIndicator,1);
        memcpy(PofData+1,&ID,4);
        dataSize+=5;
        int varcharLength;
        varcharLength=tableName.length();
        memcpy(PofData+dataSize,&varcharLength,4);
        memcpy(PofData+9,tableName.c_str(),varcharLength);
        dataSize=dataSize+4+varcharLength;

        varcharLength=fileName.length();
        memcpy(PofData+dataSize,&varcharLength,4);
        memcpy(PofData+dataSize+4,fileName.c_str(),varcharLength);

        RID rid;
        rc=rbfm->insertRecord(fileHandle,tablesRecordDescriptor,data,rid);
        if(rc==-1){return -1;}
        free(data);
        rbfm->closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::insertRecordToColumns(int table_id, const vector<Attribute> recordDescriptor)
    {
        FileHandle fileHandle;
        RC rc= rbfm->openFile("Columns",fileHandle);
        if(rc==-1){return -1;}
        void * data=malloc(500);
        char * PofData=(char *) data;
        int position=0;
        char nullindicator=0;

        for (Attribute attr:recordDescriptor)
        {
            int recordSize=0;
            memcpy(PofData,&nullindicator,1);
            memcpy(PofData+1,&table_id,4);
            recordSize+=5;
            int varcharLength;
            varcharLength=attr.name.length();
            memcpy(PofData+recordSize,&varcharLength,4);
            memcpy(PofData+9,attr.name.c_str(),varcharLength);
            recordSize=recordSize+4+varcharLength;
            memcpy(PofData+recordSize,&attr.type,4);
            memcpy(PofData+recordSize+4,&attr.length,4);
            memcpy(PofData+recordSize+8,&position,4);
            recordSize+=12;
            position+=1;
            RID rid;
            rbfm->insertRecord(fileHandle,columnsRecordDescriptor,data,rid);
        }
        rbfm->closeFile(fileHandle);
        free(data);
        return 0;
    }

    RC RelationManager::initializeTablesDescriptor()
    {
        //generate recordDescriptor for tables
        Attribute attr;
        attr.name="Id";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        tablesRecordDescriptor.push_back(attr);
        attr.name="Name";
        attr.type=TypeVarChar;
        attr.length=(AttrLength)VARCHAR_LENGTH;
        tablesRecordDescriptor.push_back(attr);
        attr.name="Filename";
        attr.type=TypeVarChar;
        attr.length=(AttrLength)VARCHAR_LENGTH;
        tablesRecordDescriptor.push_back(attr);
        return 0;
    }

    RC RelationManager::initializeColumnsDescriptor()
    {
        Attribute attr;
        attr.name="Table_id";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        columnsRecordDescriptor.push_back(attr);
        attr.name="Attribute_name";
        attr.type=TypeVarChar;
        attr.length=(AttrLength)VARCHAR_LENGTH;
        columnsRecordDescriptor.push_back(attr);
        attr.name="Type";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        columnsRecordDescriptor.push_back(attr);
        attr.name="Length";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        columnsRecordDescriptor.push_back(attr);
        attr.name="Position";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        columnsRecordDescriptor.push_back(attr);
        return 0;
    }

    RC RelationManager::destroyFile(const std::string &fileName)
    {
        rbfm->destroyFile(fileName);
        return 0;
    }

} // namespace PeterDB