#include "src/include/qe.h"

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->input=input;
        this->condition=condition;
        this->input->getAttributes(filterAttrs);
    }

    Filter::~Filter() {

    }

    RC Filter::getNextTuple(void *data) {
        while(input->getNextTuple(data)!=QE_EOF)
        {
            void * filterValue= malloc(PAGE_SIZE);
            RC rc= getFilterValue(filterAttrs,condition.lhsAttr,filterValue,data);
            if(rc!=0){
                free(filterValue);
                return -1;}
            if(isSatisfy(filterValue)==true)
            {
                free(filterValue);
                return 0;
            }

            free(filterValue);
        }
        return QE_EOF;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs= this->filterAttrs;
        return 0;
    }

    RC Filter::getFilterValue(std::vector<Attribute> &attrs, std::string filterName, void *filterValue,
                              const void *tupleData)
    {
        char * Pfilter=(char *)filterValue;
        char * Ptuple=(char *)tupleData;
        int fields=attrs.size();
        int nullBytesLen = ceil(fields / 8)+1;
        char * nullIndicator = (char*)malloc(nullBytesLen);
        memcpy(nullIndicator,Ptuple,nullBytesLen);
        bool isNull;// check attr is null
        short curOffset=nullBytesLen;
        for(int i=0;i<fields;i++)
        {
            int bytePosition = i / 8;
            int bitPosition = i % 8;
            char b = nullIndicator[bytePosition];
            isNull=  ((b >> (7 - bitPosition)) & 0x1);
            if(attrs[i].name==filterName)
            {
                if(!isNull)
                {
                    if(attrs[i].type==TypeVarChar)
                    {
                        int varLength;
                        memcpy(&varLength,Ptuple+curOffset,4);
                        memcpy(Pfilter,&varLength,4);
                        curOffset+=4;
                        memcpy(Pfilter+4,Ptuple+curOffset,varLength);
                        curOffset+=varLength;
                    }else
                    {
                        memcpy(Pfilter,Ptuple+curOffset,4);
                        curOffset+=4;
                    }
                    return 0;
                }
                else
                {
                    continue;
                }
            }else // just increase curOffset
            {
                if(!isNull)
                {
                    if(attrs[i].type==TypeVarChar)
                    {
                        int varLength;
                        memcpy(&varLength,Ptuple+curOffset,4);
                        curOffset+=4;
                        curOffset+=varLength;
                    }else
                    {
                        curOffset+=4;
                    }
                }else
                {
                    continue;
                }
            }
        }
        free(nullIndicator);
        return -1;
    }

    bool Filter::isSatisfy(const void *filterValue)
    {
        bool isSatisfy=false;
        char * Pfilter=(char *)filterValue;
        char * Prhs=(char *)condition.rhsValue.data;
        if(condition.rhsValue.type==TypeInt)
        {
            int attrIntValue;
            memcpy(&attrIntValue,Pfilter,4);
            int compValue;
            memcpy(&compValue,Prhs,4);

            switch (condition.op) {
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
        if(condition.rhsValue.type==TypeReal)
        {
            float attrFloatValue;
            memcpy(&attrFloatValue,Pfilter,4);
            float compValue;
            memcpy(&compValue,Prhs,4);

            switch (condition.op) {
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
        if(condition.rhsValue.type==TypeVarChar)
        {
            int attrVarcharLength;
            memcpy(&attrVarcharLength,Pfilter,4);

            int compVarcharLength;
            memcpy(&compVarcharLength,Prhs,4);

            switch (condition.op) {
                case EQ_OP:
                    isSatisfy=(string(Pfilter+4,attrVarcharLength) ==string(Prhs+4,compVarcharLength));
                    break;
                case LT_OP:
                    isSatisfy=(string(Pfilter+4,attrVarcharLength) < string(Prhs+4,compVarcharLength));
                    break;
                case LE_OP:
                    isSatisfy=(string(Pfilter+4,attrVarcharLength) <=string(Prhs+4,compVarcharLength));
                    break;
                case GT_OP:
                    isSatisfy=(string(Pfilter+4,attrVarcharLength) > string(Prhs+4,compVarcharLength));
                    break;
                case GE_OP:
                    isSatisfy=(string(Pfilter+4,attrVarcharLength) >= string(Prhs+4,compVarcharLength));
                    break;
                case NE_OP:
                    isSatisfy=(string(Pfilter+4,attrVarcharLength) != string(Prhs+4,compVarcharLength));
                    break;
            }
        }
        if(condition.op==NO_OP)
        {
            isSatisfy= true;
        }
        return isSatisfy;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        this->input=input;
        this->attrNames=attrNames;
        input->getAttributes(this->projectAttrs);
    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        void * projectValue= malloc(PAGE_SIZE);
        while(input->getNextTuple(projectValue)!=QE_EOF)
        {
            RC rc=getProjectValues(data,projectValue);
            if(rc!=0){ free(projectValue); return -1;}
            free(projectValue);
            return 0;
        }
        free(projectValue);
        return QE_EOF;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();

        for(string attrName:attrNames)
        {
            for(Attribute pattr:projectAttrs)
            {
                if(pattr.name==attrName)
                {
                    attrs.push_back(pattr);
                    break;
                }
            }
        }

        return 0;
    }

    RC Project::getProjectValues(const void *data, void *projectValues)
    {
        char * Pproject=(char *)projectValues;
        char * Pdata=(char *)data;
        int fields=projectAttrs.size();
        int nullBytesLen = ceil(fields / 8)+1;
        char * nullIndicator = (char*)malloc(nullBytesLen);
        memcpy(nullIndicator,Pproject,nullBytesLen);
        bool isNull;// check attr is null

        int projectFields=this->attrNames.size();
        int projectNullBytesLen = ceil(projectFields / 8)+1;
        char * projectNullIndicator = (char*)malloc(projectNullBytesLen);
        memset(projectNullIndicator,0, projectNullBytesLen);
        // project null check to fill

        short dataCurrOffset=projectNullBytesLen;

        for(int j=0;j<projectFields;j++) {
            int count=0;
            short curOffset=nullBytesLen;
            for (int i = 0; i < fields; i++) {
                int bytePosition = i / 8;
                int bitPosition = i % 8;
                char b = nullIndicator[bytePosition];
                isNull = ((b >> (7 - bitPosition)) & 0x1);
                if (projectAttrs[i].name == attrNames[j]) {
                    if (!isNull) {
                        if (projectAttrs[i].type == TypeVarChar) {
                            int varLength;
                            memcpy(&varLength, Pproject + curOffset, 4);
                            memcpy(Pdata+dataCurrOffset, &varLength, 4);
                            curOffset += 4;
                            dataCurrOffset+=4;
                            memcpy(Pdata + dataCurrOffset, Pproject + curOffset, varLength);
                            curOffset += varLength;
                            dataCurrOffset+=varLength;
                        } else {
                            memcpy(Pdata+dataCurrOffset, Pproject + curOffset, 4);
                            curOffset += 4;
                            dataCurrOffset+=4;
                        }
                    } else {
                        int byteIndex = j / 8;
                        int bitIndex = j % 8;
                        projectNullIndicator[byteIndex] += pow(2, 7-bitIndex);
                    }
                    break;
                } else // just increase curOffset
                {
                    if (!isNull) {
                        if (projectAttrs[i].type == TypeVarChar) {
                            int varLength;
                            memcpy(&varLength, Pproject + curOffset, 4);
                            curOffset += 4;
                            curOffset += varLength;
                        } else {
                            curOffset += 4;
                        }
                    } else {
                        continue;
                    }
                }
                count+=1;
            }
            if(count==fields){return -1;}// no attr is matched
        }
        memcpy(Pdata,projectNullIndicator,projectNullBytesLen);
        free(projectNullIndicator);
        free(nullIndicator);
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->leftInput=leftIn;
        this->rightInput=rightIn;
        this->condition=condition;
        this->numPages=numPages;
        this->leftInput->getAttributes(leftAttrs);
        this->rightInput->getAttributes(rightAttrs);
        this->curBlockSize=0;
        this->blockBuffer= malloc(numPages*PAGE_SIZE);
        this->rightTupleBuffer= malloc(PAGE_SIZE);
        this->rightTableScanEnd= false;
        this->tupleOffsetArrayCount=0;
        this->leftTableScanEnd= false;
        this->scanNextRightTuple= true;
        this->blockLoaded= false;
    }

    BNLJoin::~BNLJoin() {
        free(this->leftTupleBuffer);
        free(this->rightTupleBuffer);
        free(this->blockBuffer);
    }

    RC BNLJoin::getNextTuple(void *data) {
        while (leftTableScanEnd != true && blockLoaded != false) {

            if (leftTableScanEnd == false && blockLoaded == false && rightTableScanEnd == true) {
                getNextBlock();
            }
            else if (leftTableScanEnd == false & blockLoaded == true && rightTableScanEnd == false) {
                if(scanNextRightTuple=true) {
                    if (this->rightInput->getNextTuple(rightTupleBuffer) != RM_EOF) {
                        // get right filter value
                        char *PrightTuple = (char *) rightTupleBuffer;
                        AttrType filterType;
                        int tupleLength = 0;
                        void *filterValue = malloc(PAGE_SIZE);
                        getAttrOffsetAndTupleLength(leftAttrs, condition.rhsAttr, filterValue, PrightTuple, tupleLength,
                                                    filterType);

                        // get corresponding left tuple offset from map
                        int lefTupleOffset;
                        if (filterType == TypeInt) {
                            int rightIntKey;
                            memcpy(&rightIntKey, (char *) filterValue, 4);
                            auto it = intHashTable.find(rightIntKey);
                            if (it != intHashTable.end()) {

                                if(tupleOffsetArrayCount!=it->second.size()-1)
                                {
                                    mergeDatas();
                                    tupleOffsetArrayCount+=1;   // wait for other merge in vector
                                    scanNextRightTuple= false;  // do not scan next tuple
                                    return 0;
                                }else
                                {
                                    mergeDatas();
                                    tupleOffsetArrayCount=0;
                                    scanNextRightTuple= true;
                                    return 0;
                                }
                            } else {// not fount key
                                return QE_EOF;
                            }
                        } else if (filterType == TypeReal) {
                            float rightRealKey;
                            memcpy(&rightRealKey, (char *) filterValue, 4);
                            auto it = realHashTable.find(rightRealKey);
                            if (it != realHashTable.end()) {
                                if(tupleOffsetArrayCount!=it->second.size()-1)
                                {
                                    mergeDatas();
                                    tupleOffsetArrayCount+=1;   // wait for other merge in vector
                                    scanNextRightTuple= false;  // do not scan next tuple
                                    return 0;
                                }else
                                {
                                    mergeDatas();
                                    tupleOffsetArrayCount=0;
                                    scanNextRightTuple= true;
                                    return 0;
                                }
                            } else {//not found key
                                return QE_EOF;
                            }
                        } else if (filterType == TypeVarChar) {
                            string varcharKey;
                            int varlength;
                            memcpy(&varlength, (char *) filterValue, 4);
                            varcharKey = string((char *) filterValue + 4, varlength);
                            auto it = varcharHashTable.find(varcharKey);
                            if (it != varcharHashTable.end()) {
                                if(tupleOffsetArrayCount!=it->second.size()-1)
                                {
                                    mergeDatas();
                                    tupleOffsetArrayCount+=1;   // wait for other merge in vector
                                    scanNextRightTuple= false;  // do not scan next tuple
                                    return 0;
                                }else
                                {
                                    mergeDatas();
                                    tupleOffsetArrayCount=0;
                                    scanNextRightTuple= true;
                                    return 0;
                                }
                            } else {//not found key
                                return QE_EOF;
                            }
                        }
                    } else {
                        // reset right iterator and reload block
                        rightInput->setIterator();
                        cleanBlock();
                        rightTableScanEnd = true;
                    }
                }else  // do not get next tuple
                {
                    // get right filter value
                    char *PrightTuple = (char *) rightTupleBuffer;
                    AttrType filterType;
                    int tupleLength = 0;
                    void *filterValue = malloc(PAGE_SIZE);
                    getAttrOffsetAndTupleLength(leftAttrs, condition.rhsAttr, filterValue, PrightTuple, tupleLength,
                                                filterType);

                    // get corresponding left tuple offset from map
                    int lefTupleOffset;
                    if (filterType == TypeInt) {
                        int rightIntKey;
                        memcpy(&rightIntKey, (char *) filterValue, 4);
                        auto it = intHashTable.find(rightIntKey);
                        if (it != intHashTable.end()) {

                            if(tupleOffsetArrayCount!=it->second.size()-1)
                            {
                                mergeDatas();
                                tupleOffsetArrayCount+=1;   // wait for other merge in vector
                                scanNextRightTuple= false;  // do not scan next tuple
                                return 0;
                            }else
                            {
                                mergeDatas();
                                tupleOffsetArrayCount=0;
                                scanNextRightTuple= true;
                                return 0;
                            }
                        } else {// not fount key
                            return QE_EOF;
                        }
                    } else if (filterType == TypeReal) {
                        float rightRealKey;
                        memcpy(&rightRealKey, (char *) filterValue, 4);
                        auto it = realHashTable.find(rightRealKey);
                        if (it != realHashTable.end()) {
                            if(tupleOffsetArrayCount!=it->second.size()-1)
                            {
                                mergeDatas();
                                tupleOffsetArrayCount+=1;   // wait for other merge in vector
                                scanNextRightTuple= false;  // do not scan next tuple
                                return 0;
                            }else
                            {
                                mergeDatas();
                                tupleOffsetArrayCount=0;
                                scanNextRightTuple= true;
                                return 0;
                            }
                        } else {//not found key
                            return QE_EOF;
                        }
                    } else if (filterType == TypeVarChar) {
                        string varcharKey;
                        int varlength;
                        memcpy(&varlength, (char *) filterValue, 4);
                        varcharKey = string((char *) filterValue + 4, varlength);
                        auto it = varcharHashTable.find(varcharKey);
                        if (it != varcharHashTable.end()) {
                            if(tupleOffsetArrayCount!=it->second.size()-1)
                            {
                                mergeDatas();
                                tupleOffsetArrayCount+=1;   // wait for other merge in vector
                                scanNextRightTuple= false;  // do not scan next tuple
                                return 0;
                            }else
                            {
                                mergeDatas();
                                tupleOffsetArrayCount=0;
                                scanNextRightTuple= true;
                                return 0;
                            }
                        } else {//not found key
                            return QE_EOF;
                        }
                    }
                }

            }
        }
        // end join
        return QE_EOF;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        for(Attribute attr:leftAttrs)
        {
            attrs.push_back(attr);
        }
        for(Attribute attr:rightAttrs)
        {
            attrs.push_back(attr);
        }
        return 0;
    }


    RC BNLJoin::getNextBlock()
    {
        char * Pblock=(char *)blockBuffer;
        while(this->leftInput->getNextTuple(Pblock+curBlockSize)!=RM_EOF )
        {
            AttrType filterType;
            int tupleLength=0;
            void * filterValue= malloc(PAGE_SIZE);
            getAttrOffsetAndTupleLength(leftAttrs,condition.lhsAttr,filterValue,Pblock+curBlockSize,tupleLength,filterType);

            // if block is overflow
            if( (curBlockSize+tupleLength) > (numPages*PAGE_SIZE) )
            {
                free(filterValue);
                blockLoaded= true;
                return 0;
            }

            // update map
            if(filterType==TypeInt)
            {
                int intKey;
                memcpy(&intKey,(char *)filterValue,4);
                // if key in map or not
                auto it = intHashTable.find(intKey);
                if(it!=intHashTable.end())
                {
                    it->second.push_back(curBlockSize);
                }else
                {
                    vector<int> tupleOffset;
                    tupleOffset.push_back(curBlockSize);
                    intHashTable.emplace(intKey,tupleOffset);
                }
            }else if (filterType==TypeReal)
            {
                float realKey;
                memcpy(&realKey,(char *)filterValue,4);
                // if key in map or not
                auto it =realHashTable.find(realKey);
                if(it!=realHashTable.end())
                {
                    it->second.push_back(curBlockSize);
                }else
                {
                    vector<int> tupleOffset;
                    tupleOffset.push_back(curBlockSize);
                    realHashTable.emplace(realKey,tupleOffset);
                }
            }else if (filterType==TypeVarChar)
            {
                string varcharKey;
                int varlength;
                memcpy(&varlength,(char *)filterValue,4);
                varcharKey=string((char *)filterValue+4,varlength);
                // if key in map or not
                auto it = varcharHashTable.find(varcharKey);
                if(it!=varcharHashTable.end())
                {
                    it->second.push_back(curBlockSize);
                }else
                {
                    vector<int> tupleOffset;
                    tupleOffset.push_back(curBlockSize);
                    varcharHashTable.emplace(varcharKey,tupleOffset);
                }
            }
            curBlockSize+=tupleLength;
            free(filterValue);
        }
        if(curBlockSize>0)// not full
        {blockLoaded= true;}
        else{blockLoaded= false;}
        leftTableScanEnd= true;
        return 0;
    }

    RC BNLJoin::getAttrOffsetAndTupleLength(std::vector<Attribute> &attrs, std::string filterName, void *filterValue,
                               const void *tupleData,int & tupleLength,AttrType &filterType)
    {
        char * Pfilter=(char *)filterValue;
        char * Ptuple=(char *)tupleData;
        int fields=attrs.size();
        int nullBytesLen = ceil(fields / 8)+1;
        char * nullIndicator = (char*)malloc(nullBytesLen);
        memcpy(nullIndicator,Ptuple,nullBytesLen);
        bool isNull;// check attr is null
        int curOffset=nullBytesLen;
        for(int i=0;i<fields;i++)
        {
            int bytePosition = i / 8;
            int bitPosition = i % 8;
            char b = nullIndicator[bytePosition];
            isNull=  ((b >> (7 - bitPosition)) & 0x1);
            if(attrs[i].name==filterName)
            {
                if(!isNull)
                {
                    if(attrs[i].type==TypeVarChar)
                    {
                        filterType=TypeVarChar;
                        int varLength;
                        memcpy(&varLength,Ptuple+curOffset,4);
                        memcpy(Pfilter,&varLength,4);
                        curOffset+=4;
                        memcpy(Pfilter+4,Ptuple+curOffset,varLength);
                        curOffset+=varLength;
                    }else
                    {
                        if(attrs[i].type==TypeInt)
                            {filterType=TypeInt;}
                        else
                        {filterType=TypeReal;}
                        memcpy(Pfilter,Ptuple+curOffset,4);
                        curOffset+=4;
                    }
                }
                else
                {
                    continue;
                }
            }else // just increase curOffset
            {
                if(!isNull)
                {
                    if(attrs[i].type==TypeVarChar)
                    {
                        int varLength;
                        memcpy(&varLength,Ptuple+curOffset,4);
                        curOffset+=4;
                        curOffset+=varLength;
                    }else
                    {
                        curOffset+=4;
                    }
                }else
                {
                    continue;
                }
            }
        }
        free(nullIndicator);
        memcpy(&tupleLength,&curOffset,4);
        return 0;
    }

    RC BNLJoin::mergeDatas(const void *leftValue, const void *rightValue, void *data, std::vector<Attribute> &leftAttrs,
                           std::vector<Attribute> &rightAttrs)
   {
        char * Pleft=(char *)leftValue;
        char * Pright=(char *)rightValue;
        char * Pdata=(char *)data;

        int leftFields=rightAttrs.size();
        int leftNullBytesLen = ceil(leftFields / 8)+1;
        char * leftNullIndicator = (char*)malloc(leftNullBytesLen);
        memcpy(leftNullIndicator,Pleft,leftNullBytesLen);
        bool isNullleft;// check attr is null
        int leftCurOffset=leftNullBytesLen;

        int rightFields=rightAttrs.size();
        int rightNullBytesLen = ceil(rightFields / 8)+1;
        char * rightNullIndicator = (char*)malloc(rightNullBytesLen);
        memcpy(rightNullIndicator,Pright,rightNullBytesLen);
        bool isNullright;// check attr is null
        int rightCurOffset=rightNullBytesLen;

        int dataFields=this->leftAttrs.size()+this->rightAttrs.size();
        int dataNullBytesLen = ceil(dataFields / 8)+1;
        char * dataNullIndicator = (char*)malloc(dataNullBytesLen);
        memset(dataNullIndicator,0, dataNullBytesLen);

        for(int i=0;i<leftFields;i++)
        {
            int bytePosition = i / 8;
            int bitPosition = i % 8;
            char b = leftNullIndicator[bytePosition];
            isNullleft=  ((b >> (7 - bitPosition)) & 0x1);
            if(isNullleft)
            {
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                dataNullIndicator[byteIndex] += pow(2, 7-bitIndex);
            }
        }
        for(int i=0;i<rightFields;i++)
        {

        }
        free(leftNullIndicator);
        free(rightNullIndicator);
        free(dataNullIndicator);
   }

    RC BNLJoin::cleanBlock()
    {
        blockLoaded= false;
        memset(blockBuffer,0,numPages*PAGE_SIZE);
        return 0;
    }


    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB
