#include "src/include/rm.h"

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

        //IndexManager& ix = IndexManager::instance();
        //this->ix = &ix;
    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        RC rc=rbfm->createFile("Tables");
        if(rc==-1){return -1;}
        insertRecordToTables(0,"Tables","Tables");
        insertRecordToTables(1,"Columns","Columns");

        rc=rbfm->createFile("Columns");
        if(rc==-1){return -1;}
        insertRecordToColumns(0,tablesRecordDescriptor);
        insertRecordToColumns(1,columnsRecordDescriptor);
        
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        return -1;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        return -1;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        return -1;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        return -1;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return -1;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    RC RelationManager::insertRecordToTables(int ID, const string &tableName, const string &fileName)
    {
        FileHandle fileHandle;
        RC rc= rbfm->openFile("Tables",fileHandle);
        if(rc==-1){return -1;}
        void * data= malloc(1+4+2+50+50);
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
        void * data=malloc(PAGE_SIZE);
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
            memcpy(PofData+recordSize+4,&position,4);
            recordSize+=8;
            position+=1;
            RID rid;
            RC rc = rbfm->insertRecord(fileHandle,columnsRecordDescriptor,data,rid);
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
        attr.name="Position";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        columnsRecordDescriptor.push_back(attr);
    }

} // namespace PeterDB