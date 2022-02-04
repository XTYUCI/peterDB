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
        // initialize the Tables and Columns
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
        // destroy Tables and Columns file
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
        // test if the catalog exist
        FILE *fp= fopen("Tables","rb");
        if (fp)
        {
            fclose(fp);
        }else {return -1;}

        FILE *fp2= fopen("Columns","rb");
        if (fp2)
        {
            fclose(fp2);
        }else {return -1;}

        int maxTablesIndex=getTablesMaxIndex();
        // insert record info in Tables and Cloumns table
        RC rc= insertRecordToTables(maxTablesIndex+1,tableName,tableName);
        if(rc==-1){return -1;}

        rc= insertRecordToColumns(maxTablesIndex+1,attrs);
        if(rc==-1){return -1;}
        // create the file with given table name
        rc= rbfm->createFile(tableName);
        if(rc==-1){return -1;}
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if(tableName=="Tables"||tableName=="Columns"){return -1;}

        RID rid;
        //delete record in Tables
        int deleteID= getTableId(tableName,rid); // get delete id
        if(deleteID==-1){return -2;} // tablename not in tables
        RC rc= deleteTuple("Tables",rid);
        if(rc==-1){return -1;}

        // delete record in Columns
        RM_ScanIterator rm_ScanIterator;
        vector<string> attributeNames;
        attributeNames.emplace_back("column-name");
        string condAttr = "table-id";
        CompOp compOp = EQ_OP;
        void * data= malloc(PAGE_SIZE);

        RID deletedColumnRid;
        // Scan the Cloumns Table and delete corresponding table-id record
        scan("Columns",condAttr,compOp,&deleteID,attributeNames,rm_ScanIterator);
        while(rm_ScanIterator.getNextTuple(deletedColumnRid,data)!=RM_EOF)
        {
            rc = deleteTuple("Columns",deletedColumnRid);
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
            attributeNames.push_back("column-name");
            attributeNames.push_back("column-type");
            attributeNames.push_back("column-length");
            //generate attributeNames
            string condAttr = "table-id";
            CompOp compOp = EQ_OP;
            RM_ScanIterator rmScanIterator;
            scan("Columns",condAttr,compOp,&tableId,attributeNames,rmScanIterator);
            void * data= malloc(PAGE_SIZE);
            char * Pdata=(char *)data;
            while(rmScanIterator.getNextTuple(rid,data)!=RM_EOF)
            {
                Attribute attribute;                                //scan the Columns, if the table-id meet, push the retrieved attribute inFo into the attr
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
        rc=rbfm->readAttribute(fileHandle,recordDescriptor,rid,attributeName,data);   // for debug
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

        RC rc=rbfm->openFile(tableName,rm_ScanIterator.rbfm_scanIterator.fileHandle);
        if(rc==-1){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);  // get attribute if the scanning table
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
        rbfm_scanIterator.close();
        rbfm_scanIterator.fileHandle.closeFile();
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
        attributeNames.emplace_back("table-id");
         string condAttr = "";
         CompOp compOp = NO_OP;
        RM_ScanIterator rm_scanIterator;
        scan("Tables",condAttr,compOp, nullptr,attributeNames,rm_scanIterator);
        int maxId=0;                      // Scan the Tables and get the total Id
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
        // get the table-id of corresponding tableName
        int tableId=-1;
        vector<string> attributeNames;
        attributeNames.emplace_back("table-id");
        string condAttr = "table-name";
        CompOp compOp = EQ_OP;
        int nameLength=tableName.length();
        void * value= malloc(4+nameLength);
        char * PofValue=(char *) value;
        memcpy(PofValue,&nameLength,4);
        memcpy(PofValue+4,tableName.c_str(),nameLength);
        RM_ScanIterator rm_ScanIterator;
        RC rc=scan("Tables",condAttr,compOp,value,attributeNames,rm_ScanIterator);
        if(rc==-1){return -1;}                       // Scan the Tables table
        void * data= malloc(5);
        char * PofData=(char *) data;
        if(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF)
        {
            memcpy(&tableId,PofData+1,4);           // if found, set the tableId
        }
        rm_ScanIterator.close();
        free(data);
        free(value);
        return tableId;
    }

    RC RelationManager::insertRecordToTables(int ID, const string &tableName, const string &fileName)
    {
        void * data= malloc(1024);
        char * PofData=(char *) data;
        int dataSize=0;
        char nullIndicator=0;
        memcpy(PofData,&nullIndicator,1);    // prepare the Tables record DATA
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
        // insert the record into the Tables table
        RID rid;
        RC rc= insertTuple("Tables",data,rid);
        if(rc==-1){return -1;}
        free(data);
        return 0;
    }

    RC RelationManager::insertRecordToColumns(int table_id, const vector<Attribute> recordDescriptor)
    {
        void * data=malloc(500);
        char * PofData=(char *) data;
        int position=1;
        char nullindicator=0;

        for (int i = 0; i < recordDescriptor.size(); i++)   // loop all the attribute and insert the record to Cloumns table
        {
            Attribute attr = recordDescriptor[i];
            // prepare  the Columns record
            int recordSize=0;
            memcpy(PofData,&nullindicator,1);
            memcpy(PofData+1,&table_id,4);
            recordSize+=5;
            int varcharLength;
            varcharLength=attr.name.length();             // prepare  the Columns record
            memcpy(PofData+recordSize,&varcharLength,4);
            memcpy(PofData+9,attr.name.c_str(),varcharLength);
            recordSize=recordSize+4+varcharLength;
            memcpy(PofData+recordSize,&attr.type,4);
            memcpy(PofData+recordSize+4,&attr.length,4);
            memcpy(PofData+recordSize+8,&position,4);
            recordSize+=12;
            position+=1;
            RID rid;
            RC rc =insertTuple("Columns",data,rid);
            if(rc==-1){return -1;}
        }
        free(data);
        return 0;
    }

    RC RelationManager::initializeTablesDescriptor()
    {
        //generate recordDescriptor for Tables table
        Attribute attr;
        attr.name="table-id";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        tablesRecordDescriptor.push_back(attr);
        attr.name="table-name";
        attr.type=TypeVarChar;
        attr.length=(AttrLength)VARCHAR_LENGTH;
        tablesRecordDescriptor.push_back(attr);
        attr.name="file-name";
        attr.type=TypeVarChar;
        attr.length=(AttrLength)VARCHAR_LENGTH;
        tablesRecordDescriptor.push_back(attr);
        return 0;
    }

    RC RelationManager::initializeColumnsDescriptor()
    {
        //generate recordDescriptor for Columns table
        Attribute attr;
        attr.name="table-id";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        columnsRecordDescriptor.push_back(attr);
        attr.name="column-name";
        attr.type=TypeVarChar;
        attr.length=(AttrLength)VARCHAR_LENGTH;
        columnsRecordDescriptor.push_back(attr);
        attr.name="column-type";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        columnsRecordDescriptor.push_back(attr);
        attr.name="column-length";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        columnsRecordDescriptor.push_back(attr);
        attr.name="column-position";
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