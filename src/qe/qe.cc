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
        this->rightTableScanEnd= true;
        this->tupleOffsetArrayIndex=0;
        this->leftTableScanEnd= false;
        this->scanNextRightTuple= true;
        this->blockLoaded= false;
        this->firstLoad= true;
    }

    BNLJoin::~BNLJoin() {
        free(this->rightTupleBuffer);
        free(this->blockBuffer);
    }

    RC BNLJoin::getNextTuple(void *data) {
        if(firstLoad)
        {
            getNextBlock();
            firstLoad= false;
        }
        while (leftTableScanEnd == false && blockLoaded == true) {

            if ( blockLoaded == true && rightTableScanEnd == false) {
                if(scanNextRightTuple==true) {
                    if (this->rightInput->getNextTuple(rightTupleBuffer) != RM_EOF) {
                        // get right filter value
                        char *PrightTuple = (char *) rightTupleBuffer;
                        AttrType filterType;
                        int tupleLength = 0;
                        void *filterValue = malloc(PAGE_SIZE);
                        getAttrOffsetAndTupleLength(rightAttrs, condition.rhsAttr, filterValue, PrightTuple, tupleLength,
                                                    filterType);

                        // get corresponding left tuple offset from map
                        if (filterType == TypeInt) {
                            int rightIntKey;
                            memcpy(&rightIntKey, (char *) filterValue, 4);
                            auto it = intHashTable.find(rightIntKey);
                            if (it != intHashTable.end()) {

                                if(tupleOffsetArrayIndex!=it->second.size()-1)
                                {
                                    int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                    mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                    tupleOffsetArrayIndex+=1;   // wait for other merge in vector
                                    scanNextRightTuple= false;  // do not scan next tuple
                                    return 0;
                                }else
                                {
                                    int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                    mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                    tupleOffsetArrayIndex=0;
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
                                if(tupleOffsetArrayIndex!=it->second.size()-1)
                                {
                                    int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                    mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                    tupleOffsetArrayIndex+=1;   // wait for other merge in vector
                                    scanNextRightTuple= false;  // do not scan next tuple
                                    return 0;
                                }else
                                {
                                    int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                    mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                    tupleOffsetArrayIndex=0;
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
                                if(tupleOffsetArrayIndex!=it->second.size()-1)
                                {
                                    int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                    mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                    tupleOffsetArrayIndex+=1;   // wait for other merge in vector
                                    scanNextRightTuple= false;  // do not scan next tuple
                                    return 0;
                                }else
                                {
                                    int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                    mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                    tupleOffsetArrayIndex=0;
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
                }else  // do not get scan next tuple of right table
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

                            if(tupleOffsetArrayIndex!=it->second.size()-1)
                            {
                                int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                tupleOffsetArrayIndex+=1;   // wait for other merge in vector
                                scanNextRightTuple= false;  // do not scan next tuple
                                return 0;
                            }else
                            {
                                int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                tupleOffsetArrayIndex=0;
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
                            if(tupleOffsetArrayIndex!=it->second.size()-1)
                            {
                                int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                tupleOffsetArrayIndex+=1;   // wait for other merge in vector
                                scanNextRightTuple= false;  // do not scan next tuple
                                return 0;
                            }else
                            {
                                int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                tupleOffsetArrayIndex=0;
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
                            if(tupleOffsetArrayIndex!=it->second.size()-1)
                            {
                                int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                tupleOffsetArrayIndex+=1;   // wait for other merge in vector
                                scanNextRightTuple= false;  // do not scan next tuple
                                return 0;
                            }else
                            {
                                int leftTupleOffset=it->second[tupleOffsetArrayIndex];
                                mergeDatas((char *)blockBuffer+leftTupleOffset,PrightTuple,data,leftAttrs,rightAttrs);
                                tupleOffsetArrayIndex=0;
                                scanNextRightTuple= true;
                                return 0;
                            }
                        } else {//not found key
                            return QE_EOF;
                        }
                    }
                }

            }
            if ( blockLoaded == false && rightTableScanEnd == true) {
                getNextBlock();  // reload block
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
                rightTableScanEnd= false;
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
        {blockLoaded= true;
            rightTableScanEnd= false;}
        else{blockLoaded= false;}
        if(!firstLoad) {
            leftTableScanEnd = true;
        }
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
        //left null
        int leftFields=rightAttrs.size();
        int leftNullBytesLen = ceil(leftFields / 8)+1;
        char * leftNullIndicator = (char*)malloc(leftNullBytesLen);
        memcpy(leftNullIndicator,Pleft,leftNullBytesLen);
        bool isNullleft;// check attr is null
        int leftCurOffset=leftNullBytesLen;
        // right null
        int rightFields=rightAttrs.size();
        int rightNullBytesLen = ceil(rightFields / 8)+1;
        char * rightNullIndicator = (char*)malloc(rightNullBytesLen);
        memcpy(rightNullIndicator,Pright,rightNullBytesLen);
        bool isNullright;// check attr is null
        int rightCurOffset=rightNullBytesLen;
        //data null
        int dataFields=this->leftAttrs.size()+this->rightAttrs.size();
        int dataNullBytesLen = ceil(dataFields / 8)+1;
        char * dataNullIndicator = (char*)malloc(dataNullBytesLen);
        memset(dataNullIndicator,0, dataNullBytesLen);
        int dataCurOffset=dataNullBytesLen;
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
            }else
            {
                if(leftAttrs[i].type==TypeVarChar)
                {
                    int varLength;
                    memcpy(&varLength,Pleft+leftCurOffset,4);
                    memcpy(Pdata+dataCurOffset,&varLength,4);
                    leftCurOffset+=4;
                    dataCurOffset+=4;
                    memcpy(Pdata+dataCurOffset,Pleft+leftCurOffset,varLength);
                    leftCurOffset+=varLength;
                    dataCurOffset+=varLength;
                }else
                {
                    memcpy(Pdata+dataCurOffset,Pleft+leftCurOffset,4);
                    leftCurOffset+=4;
                    dataCurOffset+=4;
                }
            }
        }
        for(int i=0;i<rightFields;i++)
        {
            int bytePosition = i / 8;
            int bitPosition = i % 8;
            char b = leftNullIndicator[bytePosition];
            isNullright=  ((b >> (7 - bitPosition)) & 0x1);
            if(isNullright)
            {
                int byteIndex = (i+leftFields) / 8;
                int bitIndex = (i+leftFields) % 8;
                dataNullIndicator[byteIndex] += pow(2, 7-bitIndex);
            }else
            {
                if(rightAttrs[i].type==TypeVarChar)
                {
                    int varLength;
                    memcpy(&varLength,Pright+rightCurOffset,4);
                    memcpy(Pdata+dataCurOffset,&varLength,4);
                    rightCurOffset+=4;
                    dataCurOffset+=4;
                    memcpy(Pdata+dataCurOffset,Pright+rightCurOffset,varLength);
                    rightCurOffset+=varLength;
                    dataCurOffset+=varLength;
                }else
                {
                    memcpy(Pdata+dataCurOffset,Pright+rightCurOffset,4);
                    rightCurOffset+=4;
                    dataCurOffset+=4;
                }
            }
        }

        memcpy(Pdata,dataNullIndicator,dataNullBytesLen);
        free(leftNullIndicator);
        free(rightNullIndicator);
        free(dataNullIndicator);
   }

    RC BNLJoin::cleanBlock()
    {
        blockLoaded= false;
        memset(blockBuffer,0,numPages*PAGE_SIZE);
        curBlockSize=0;
        return 0;
    }


    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        this->leftInput=leftIn;
        this->rightInput=rightIn;
        this->condition=condition;
        this->leftInput->getAttributes(leftAttrs);
        this->rightInput->getAttributes(rightAttrs);
        this->leftTupleBuffer= malloc(PAGE_SIZE);
        this->rightTupleBuffer= malloc(PAGE_SIZE);
        this->leftTableScanEnd=false;

    }

    INLJoin::~INLJoin() {
        free(leftTupleBuffer);
        free(rightTupleBuffer);
    }

    RC INLJoin::getNextTuple(void *data) {
        while(leftTableScanEnd== false) { // stop when left table scan end

                if(leftInput->getNextTuple(leftTupleBuffer) != RM_EOF) {
                    void *leftAttrValue = malloc(PAGE_SIZE);
                    RC rc = getFilterValue(leftAttrs, condition.lhsAttr, leftAttrValue, leftTupleBuffer);
                    if (rc != 0) {
                        free(leftAttrValue);
                        return -1;
                    }
                    rightInput->setIterator(leftAttrValue, leftAttrValue, true, true);
                    if(rightInput->getNextTuple(rightTupleBuffer)!=IX_EOF)
                    {
                        // find the right tuple through index

                        mergeDatas(leftTupleBuffer,rightTupleBuffer,data,leftAttrs,rightAttrs);
                        free(leftAttrValue);
                        return 0;
                    }
                }else
                {
                    leftTableScanEnd= true;
                    return QE_EOF;
                }

//
        }
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
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

    RC INLJoin::getFilterValue(std::vector<Attribute> &attrs, std::string filterName, void *filterValue,
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

    RC INLJoin::mergeDatas(const void *leftValue, const void *rightValue, void *data, std::vector<Attribute> &leftAttrs,
                           std::vector<Attribute> &rightAttrs)
    {
        char * Pleft=(char *)leftValue;
        char * Pright=(char *)rightValue;
        char * Pdata=(char *)data;
        //left null
        int leftFields=rightAttrs.size();
        int leftNullBytesLen = ceil(leftFields / 8)+1;
        char * leftNullIndicator = (char*)malloc(leftNullBytesLen);
        memcpy(leftNullIndicator,Pleft,leftNullBytesLen);
        bool isNullleft;// check attr is null
        int leftCurOffset=leftNullBytesLen;
        // right null
        int rightFields=rightAttrs.size();
        int rightNullBytesLen = ceil(rightFields / 8)+1;
        char * rightNullIndicator = (char*)malloc(rightNullBytesLen);
        memcpy(rightNullIndicator,Pright,rightNullBytesLen);
        bool isNullright;// check attr is null
        int rightCurOffset=rightNullBytesLen;
        //data null
        int dataFields=this->leftAttrs.size()+this->rightAttrs.size();
        int dataNullBytesLen = ceil(dataFields / 8)+1;
        char * dataNullIndicator = (char*)malloc(dataNullBytesLen);
        memset(dataNullIndicator,0, dataNullBytesLen);
        int dataCurOffset=dataNullBytesLen;
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
            }else
            {
                if(leftAttrs[i].type==TypeVarChar)
                {
                    int varLength;
                    memcpy(&varLength,Pleft+leftCurOffset,4);
                    memcpy(Pdata+dataCurOffset,&varLength,4);
                    leftCurOffset+=4;
                    dataCurOffset+=4;
                    memcpy(Pdata+dataCurOffset,Pleft+leftCurOffset,varLength);
                    leftCurOffset+=varLength;
                    dataCurOffset+=varLength;
                }else
                {
                    memcpy(Pdata+dataCurOffset,Pleft+leftCurOffset,4);
                    leftCurOffset+=4;
                    dataCurOffset+=4;
                }
            }
        }
        for(int i=0;i<rightFields;i++)
        {
            int bytePosition = i / 8;
            int bitPosition = i % 8;
            char b = leftNullIndicator[bytePosition];
            isNullright=  ((b >> (7 - bitPosition)) & 0x1);
            if(isNullright)
            {
                int byteIndex = (i+leftFields) / 8;
                int bitIndex = (i+leftFields) % 8;
                dataNullIndicator[byteIndex] += pow(2, 7-bitIndex);
            }else
            {
                if(rightAttrs[i].type==TypeVarChar)
                {
                    int varLength;
                    memcpy(&varLength,Pright+rightCurOffset,4);
                    memcpy(Pdata+dataCurOffset,&varLength,4);
                    rightCurOffset+=4;
                    dataCurOffset+=4;
                    memcpy(Pdata+dataCurOffset,Pright+rightCurOffset,varLength);
                    rightCurOffset+=varLength;
                    dataCurOffset+=varLength;
                }else
                {
                    memcpy(Pdata+dataCurOffset,Pright+rightCurOffset,4);
                    rightCurOffset+=4;
                    dataCurOffset+=4;
                }
            }
        }

        memcpy(Pdata,dataNullIndicator,dataNullBytesLen);
        free(leftNullIndicator);
        free(rightNullIndicator);
        free(dataNullIndicator);
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
        this->aggInput=input;
        this->aggAttr=aggAttr;
        this->op=op;
        this->aggInput->getAttributes(this->attrs);
        this->tupleBuffer= malloc(PAGE_SIZE);
        sumFloat=0;
        avg=0;
        count=0;
        hasReturnAgg=false;
        firstRun= true;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {
        free(tupleBuffer);

    }

    RC Aggregate::getNextTuple(void *data) {
        if(hasReturnAgg== true){return QE_EOF;}
        while(this->aggInput->getNextTuple(tupleBuffer)!=RM_EOF)
        {
            void * filterValue= malloc(4);
            RC rc= getFilterValue(attrs,aggAttr.name,filterValue,tupleBuffer);
            if(rc!=0)
            {
                free(filterValue);
                return QE_EOF;
            }
            float fltValue=0;
            if(aggAttr.type==TypeInt)
            {
                int intValue;
                memcpy(&intValue,(char *)filterValue,4);
                fltValue= intValue;
            }else if(aggAttr.type==TypeReal) {
                memcpy(&fltValue, (char *) filterValue, 4);
            }
            if(firstRun)
            {
                maxFloat=fltValue;
                minFloat=fltValue;
                firstRun= false;
            }
            if(fltValue>=maxFloat&&firstRun== false)
            {
                maxFloat=fltValue;
            }
            if(fltValue<=minFloat&&firstRun== false)
            {
                minFloat=fltValue;
            }
            count+=1;
            sumFloat+=fltValue;
            free(filterValue);
        }
        avg=sumFloat/count;
        // put result to data
        int nullBytesLen = 1;
        char * nullIndicator = (char*)malloc(nullBytesLen);
        memset(nullIndicator, 0, nullBytesLen);
        memcpy((char *)data,nullIndicator,nullBytesLen);

        float result;
        switch (op) {
            case MIN:
            {
                result=minFloat;
                break;
            }
            case MAX:
            {
                result=maxFloat;
                break;
            }
            case COUNT:
            {
                result=count;
                break;
            }
            case SUM:
            {
                result=sumFloat;
                break;
            }
            case AVG:
            {
                result=avg;
                break;
            }
        }
        memcpy((char *)data+nullBytesLen,&result,4);
        hasReturnAgg= true;
        return 0;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        Attribute attr;
        attr=this->aggAttr;
        string opName;
        switch (op) {
            case MIN:
                opName="MIN";
                break;
            case MAX:
                opName="MAX";
                break;
            case COUNT:
                opName="COUNT";
                break;
            case SUM:
                opName="SUM";
                break;
            case AVG:
                opName="AVG";
                break;
        }
        attr.name=opName+'('+this->aggAttr.name+')';
        attr.type = TypeReal;
        attrs.push_back(attr);
        return 0;
    }

    RC Aggregate::getFilterValue(std::vector<Attribute> &attrs, std::string filterName, void *filterValue,
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

} // namespace PeterDB
