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
        initializeIndexsDescriptor();
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        this->rbfm = &rbfm;
        IndexManager& ix = IndexManager::instance();
        this->ix = &ix;
    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {

        RC rc=rbfm->createFile("Tables");
        if(rc!=0){return -1;}
        rc=rbfm->createFile("Columns");
        if(rc!=0){return -1;}
        rc=rbfm->createFile("Indexs");
        if(rc!=0){return -1;}
        // initialize the Tables and Columns and Indexs
        rc = insertRecordToTables(1,"Tables","Tables");
        if(rc!=0){return -1;}
        rc= insertRecordToTables(2,"Columns","Columns");
        if(rc!=0){return -1;}


        rc=insertRecordToColumns(1,tablesRecordDescriptor);
        if(rc!=0){return -1;}
        rc=insertRecordToColumns(2,columnsRecordDescriptor);
        if(rc!=0){return -1;}
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        // destroy Tables and Columns file
        RC rc=rbfm->destroyFile("Tables");
        if(rc!=0){return -1;}
        rc=rbfm->destroyFile("Columns");
        if(rc!=0){return -1;}
        rc=rbfm->destroyFile("Indexs");
        if(rc!=0){return -1;}
        remove("Tables");
        remove("Columns");
        remove("Indexs");
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        if(tableName=="Tables"||tableName=="Columns"||tableName=="Indexs"){return -1;}
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
        if(rc!=0){return -1;}

        rc= insertRecordToColumns(maxTablesIndex+1,attrs);
        if(rc!=0){return -1;}
        // create the file with given table name
        rc= rbfm->createFile(tableName);
        if(rc!=0){return -1;}
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if(tableName=="Tables"||tableName=="Columns"||tableName=="Indexs"){return -1;}

        //project4 delete all index file
        vector<string> indexAttrs;
        vector<RID> indexRids;
        getIndexAttrsNameANDrid(tableName,indexAttrs,indexRids);
        for(string attr:indexAttrs)
        {
            RC rc= destroyIndex(tableName,attr);
            if(rc!=0){return -1;}
        }

        RID rid;
        //delete record in Tables
        int deleteID= getTableId(tableName,rid); // get delete id
        if(deleteID==-1){return -1;} // tablename not in tables
        RC rc= deleteTuple("Tables",rid);
        if(rc!=0){return -1;}

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
            if(rc!=0){return -1;}
        }
        rm_ScanIterator.close();
        free(data);

        rc=rbfm->destroyFile(tableName);
        remove(tableName.c_str());
        if(rc!=0){return -1;}
        return 0;
    }

    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName)
    {
        FILE *fp= fopen("Indexs","rb");
        if (fp)
        {
            fclose(fp);
        }else {return -1;}
        RC rc;
        RM_ScanIterator rm_ScanIterator;
        string indexFileName=tableName+"_"+attributeName+".idx";
        // if index exist
//        rc=ix->openFile(indexFileName,ixFileHandle);
//        if(rc==0){return -1;}
        rc=ix->createFile(indexFileName);
        if(rc!=0){return -1;}
        // get attribute type
        vector<Attribute> attrs;
        Attribute thisAttr;
        rc= getAttributes(tableName,attrs);
        if(rc!=0){return -1;}
        for(Attribute attr:attrs)
        {
            if(attr.name==attributeName)
            {
                thisAttr=attr;
                break;
            }
        }
        // update indexs table
        RID rid;
        int tableID= getTableId(tableName,rid);
        rc= insertRecordToIndexs(tableID,attributeName,indexFileName);
        if(rc!=0){return -1;}

        // scan and insert all index attr
        vector<string> attributeNames;
        attributeNames.emplace_back(attributeName);
        scan(tableName,"",NO_OP, nullptr,attributeNames,rm_ScanIterator);
        void * data= malloc(PAGE_SIZE);
        RID attributeRid;
        while(rm_ScanIterator.getNextTuple(attributeRid,data)!=RM_EOF)
        {
            FileHandle fileHandle;
            IXFileHandle ixFileHandle;
            void *keyData = malloc(PAGE_SIZE);
            void * attrData = malloc(PAGE_SIZE);
            rc=rbfm->openFile(tableName,fileHandle);
            if(rc!=0){return -1;}
            rc=rbfm->readAttribute(fileHandle,attrs,attributeRid,attributeName,attrData);
            if(rc!=0){return -1;}
            if(thisAttr.type==TypeVarChar) {
                int varLength;
                memcpy(&varLength, (char *) attrData + 1,4);
                memcpy((char *)keyData,(char *) attrData + 1,4+varLength);
            }else
            {
                memcpy((char *)keyData,(char *)attrData + 1,4);
            }
            rc=ix->openFile(indexFileName,ixFileHandle);
            if(rc!=0){return -1;}
            rc=ix->insertEntry(ixFileHandle,thisAttr,keyData,attributeRid);
            if(rc!=0){return -1;}
            rc=ix->closeFile(ixFileHandle);
            if(rc!=0){return -1;}
            rc=rbfm->closeFile(fileHandle);
            if(rc!=0){return -1;}
            free(keyData);
            free(attrData);
        }
        free(data);

        return 0;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName)
    {
        string indexFileName=tableName+"_"+attributeName+".idx";
        RM_ScanIterator rm_ScanIterator;
        RC rc;

        // update indexs table
        RID deleteRid;
        int deleteID= getTableId(tableName,deleteRid); // get delete id
        if(deleteID==-1){return -1;}
        vector<string> deleteAttributeNames;
        deleteAttributeNames.emplace_back("column-name");
        string condAttr = "table-id";
        CompOp compOp = EQ_OP;
        void * Ddata= malloc(PAGE_SIZE);

        RID deletedColumnRid;
        rc=scan("Indexs",condAttr,compOp,&deleteID,deleteAttributeNames,rm_ScanIterator);
        if(rc!=0){return -1;}
        while(rm_ScanIterator.getNextTuple(deletedColumnRid,Ddata)!=RM_EOF)
        {
            rc = deleteTuple("Indexs",deletedColumnRid);
            if(rc!=0){return -1;}
        }
        rm_ScanIterator.close();
        free(Ddata);
        IXFileHandle ixFileHandle;
        rc=ix->openFile(indexFileName,ixFileHandle);
        if(rc!=0){return -1;}
        rc=ix->destroyFile(indexFileName);
        if(rc!=0){return -1;}
        rc=ix->closeFile(ixFileHandle);
        remove(indexFileName.c_str());
        if(rc!=0){return -1;}

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {

        if(tableName=="Tables"){attrs=tablesRecordDescriptor;}
        else if(tableName=="Columns"){attrs=columnsRecordDescriptor;}
        else if(tableName=="Indexs"){attrs=indexRecordRescriptor;}
        else
        {
            RID rid;
            int tableId= getTableId(tableName,rid);
            if (tableId==-1){return -1;}
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

    RC RelationManager::getIndexAttrsNameANDrid(const std::string &tableName, std::vector<string> &indexAttrs,std::vector<RID> &indexRids)
    {
        RID rid;
        int tableId= getTableId(tableName,rid);
        if (tableId==-1){return -1;}
        vector<string> indexAttributeNames;
        indexAttributeNames.emplace_back("column-name");
        string condAttr = "table-id";
        CompOp compOp = EQ_OP;
        RM_ScanIterator rmScanIterator;
        scan("Indexs",condAttr,compOp,&tableId,indexAttributeNames,rmScanIterator);
        void * data= malloc(PAGE_SIZE);
        char * Pdata=(char *)data;
        while(rmScanIterator.getNextTuple(rid,data)!=RM_EOF)
        {
            string attr;
            int varcharLength;
            memcpy(&varcharLength,Pdata+1,4);
            attr=string(Pdata+5,varcharLength);
            indexAttrs.push_back(attr);
            indexRids.push_back(rid);
        }
        free(data);
        rmScanIterator.close();
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        FileHandle fileHandle;
        RC rc= rbfm->openFile(tableName,fileHandle);
        if(rc!=0){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        rc=rbfm->insertRecord(fileHandle,recordDescriptor,data,rid);
        if(rc!=0){return -1;}
        rc=rbfm->closeFile(fileHandle);
        if(rc!=0){return -1;}

        // project4 insert associated index
        if(tableName!="Tables"&&tableName!="Columns"&&tableName=="Indexs")
        {
            vector<string> indexAttrs;
            vector<RID> indexRids;
            getIndexAttrsNameANDrid(tableName, indexAttrs,indexRids);
            rc= updateIndex(tableName,indexAttrs,recordDescriptor,indexRids,0); //0 means insert update
            if(rc!=0){return -1;}
        }
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        FileHandle fileHandle;
        RC rc= rbfm->openFile(tableName,fileHandle);
        if(rc!=0){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);

        //project4 delete associated index
        if(tableName!="Tables"&&tableName!="Columns"&&tableName=="Indexs")
        {
            vector<string> indexAttrs;
            vector<RID> indexRids;
            getIndexAttrsNameANDrid(tableName, indexAttrs,indexRids);
            rc= updateIndex(tableName,indexAttrs,recordDescriptor,indexRids,1); //1 means delete update
            if(rc!=0){return -1;}
        }

        rc=rbfm->deleteRecord(fileHandle,recordDescriptor,rid);
        if(rc!=0){return -1;}
        rc=rbfm->closeFile(fileHandle);
        if(rc!=0){return -1;}

        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        FileHandle fileHandle;
        RC rc= rbfm->openFile(tableName,fileHandle);
        if(rc!=0){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        rc=rbfm->updateRecord(fileHandle,recordDescriptor,data,rid);
        if(rc!=0){return -1;}
        rc=rbfm->closeFile(fileHandle);
        if(rc!=0){return -1;}
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        FileHandle fileHandle;
        RC rc= rbfm->openFile(tableName,fileHandle);
        if(rc!=0){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        rc=rbfm->readRecord(fileHandle,recordDescriptor,rid,data);
        if(rc!=0){return -1;}
        rc=rbfm->closeFile(fileHandle);
        if(rc!=0){return -1;}
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
        if(rc!=0){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        rc=rbfm->readAttribute(fileHandle,recordDescriptor,rid,attributeName,data);   // for debug
        if(rc!=0){return -1;}
        rc=rbfm->closeFile(fileHandle);
        if(rc!=0){return -1;}
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {

        RC rc=rbfm->openFile(tableName,rm_ScanIterator.rbfm_scanIterator.fileHandle);
        if(rc!=0){return -1;}

        vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);  // get attribute if the scanning table
        rbfm->scan(rm_ScanIterator.rbfm_scanIterator.fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfm_scanIterator);

        return 0;
    }

    RC RelationManager::indexScan(const std::string &tableName, const std::string &attributeName, const void *lowKey,
                                  const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator) {
        string indexFileName=tableName+'_'+attributeName;
        RC rc= ix->openFile(indexFileName,rm_IndexScanIterator.ixFileHandle);
        if(rc!=0){return -1;}

        // get attribute type
        vector<Attribute> attrs;
        Attribute thisAttr;
        rc= getAttributes(tableName,attrs);
        if(rc!=0){return -1;}
        for(Attribute attr:attrs)
        {
            if(attr.name==attributeName)
            {
                thisAttr=attr;
                break;
            }
        }
        ix->scan(rm_IndexScanIterator.ixFileHandle,thisAttr,lowKey,highKey,lowKeyInclusive,highKeyInclusive,rm_IndexScanIterator.ix_scanIterator);
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

    RM_IndexScanIterator::RM_IndexScanIterator() =default;

    RM_IndexScanIterator::~RM_IndexScanIterator() =default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)
    {
        RC rc=ix_scanIterator.getNextEntry(rid,key);
        if(rc==IX_EOF){return IX_EOF;}
        return 0;
    }

    RC RM_IndexScanIterator::close()
    {
        ix_scanIterator.close();
        ix_scanIterator.ixFileHandle->fileHandle.closeFile();
        return 0;
    }

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
        if(rc!=0){return -1;}                      // Scan the Tables table
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


    RC RelationManager::updateIndex(const string &tableName,std::vector<string> &indexAttrs, std::vector<Attribute> &attrs, std::vector<RID> &indexRids, int insertOrDelete)
    {
        for(Attribute attr: attrs) {
            for (int i=0;i<indexAttrs.size();i++) {
                if(attr.name==indexAttrs[i]){
                    IXFileHandle ixFileHandle;
                    FileHandle fileHandle;
                    void *keyData = malloc(PAGE_SIZE);
                    void * attrData = malloc(PAGE_SIZE);
                    rbfm->openFile(tableName,fileHandle);
                    RC rc=rbfm->readAttribute(fileHandle,attrs,indexRids[i],indexAttrs[i],attrData);
                    if(rc!=0){return -1;}
                    if(attr.type==TypeVarChar) {
                        int varLength;
                        memcpy(&varLength, (char *) attrData + 1,4);
                        memcpy((char *)keyData,(char *) attrData + 1,4+varLength);
                    }else
                    {
                        memcpy((char *)keyData,(char *) attrData + 1,4);
                    }
                    string indexFileName=tableName+"_"+indexAttrs[i]+".idx";
                    rc=ix->openFile(indexFileName,ixFileHandle);
                    if(rc!=0){return -1;}
                    if(insertOrDelete==0) { //insert
                        rc = ix->insertEntry(ixFileHandle, attr, keyData, indexRids[i]);
                        if(rc!=0){return -1;}
                    }
                    else if(insertOrDelete==1)
                    { // delete
                        rc=ix->deleteEntry(ixFileHandle,attr,keyData,indexRids[i]);
                        if(rc!=0){return -1;}
                    }
                    free(keyData);
                    free(attrData);
                    rbfm->closeFile(fileHandle);
                    rc=ix->closeFile(ixFileHandle);
                    if(rc!=0){return -1;}
                    break;
                }
            }
        }
        return 0;
    }


    RC RelationManager::insertRecordToTables(int ID, const string &tableName, const string &fileName)
    {
        void * data= malloc(PAGE_SIZE);
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
        if(rc!=0){return -1;}
        free(data);
        return 0;
    }

    RC RelationManager::insertRecordToColumns(int table_id, const vector<Attribute> recordDescriptor)
    {
        void * data=malloc(PAGE_SIZE);
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
            if(rc!=0){return -1;}
        }
        free(data);
        return 0;
    }

    RC RelationManager::insertRecordToIndexs(int table_id, const string &columnName, const string &indexFileName)
    {
        //prepare index record
        void *data= malloc(PAGE_SIZE);
        char *PofData=(char *)data;
        int dataSize=0;
        char nullIndicator=0;
        memcpy(PofData,&nullIndicator,1);
        memcpy(PofData+1,&table_id,4);
        dataSize+=5;
        int varcharLength;
        varcharLength=columnName.length();
        memcpy(PofData+dataSize,&varcharLength,4);
        memcpy(PofData+9,columnName.c_str(),varcharLength);
        dataSize=dataSize+4+varcharLength;

        varcharLength=indexFileName.length();
        memcpy(PofData+dataSize,&varcharLength,4);
        memcpy(PofData+dataSize+4,indexFileName.c_str(),varcharLength);

        RID rid;
        RC rc= insertTuple("Indexs",data,rid);
        if(rc!=0){return -1;}
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

    RC RelationManager::initializeIndexsDescriptor()
    {
        Attribute attr;
        attr.name="table-id";
        attr.type=TypeInt;
        attr.length=(AttrLength)4;
        indexRecordRescriptor.push_back(attr);
        attr.name="column-name";
        attr.type=TypeVarChar;
        attr.length=(AttrLength)VARCHAR_LENGTH;
        indexRecordRescriptor.push_back(attr);
        attr.name="index-file-name";
        attr.type=TypeVarChar;
        attr.length=(AttrLength)VARCHAR_LENGTH;
        indexRecordRescriptor.push_back(attr);
        return 0;
    }

    RC RelationManager::destroyFile(const std::string &fileName)
    {
        rbfm->destroyFile(fileName);
        return 0;
    }


} // namespace PeterDB