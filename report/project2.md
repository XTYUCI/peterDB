## Project 2 Report


### 1. Basic information
 - Team #:
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-winter22-XTYUCI
 - Student 1 UCI NetID: txie11
 - Student 1 Name: Tianyi Xie
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.
I follow the requirement in the public test
Tables:    |table-id|table-name|file-name|
Columns: |table-id|column-name|column-type |column-length|column-position|

### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.
RECORD FORMAT:
| null indicator | num of fields | filed1offset |filed2 offset | .... | record data |

- Describe how you store a null field.

Check the null indicator by the number position of the field. Store the null indicator and the null field's size will be 0. 

- Describe how you store a VarChar field.
Store the varChar field offset and its length. 


- Describe how your record design satisfies O(1) field access.
Get the field offset through the position number of field among all fields.
Use recordoffset+fieldoffset to access the field


### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.
The record is insert froM the begining of a page.
From the end of a page, there is 2 bytes recording the freebytes of that page, 2 bytes recording the number of records, and 4 bytes of each slot with corresponing records.

- Explain your slot directory design if applicable.
2 bytes for the record pointer offset which points to the begining of each record.
2 bytes for the record size.

### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?
If 0 page in the file then we apend a new page and insert the record.
Checking the free space of the last exist page if it has free spcae then we insert the record.
Iterate all exist page from the first page. if any one of them has free space then we insert the record.
No exist page has free space then we append a new a page and insert. 


- Show your hidden page(s) format design if applicable
One, the hidden page is created with the file. It store the total pagenums, read count, write count and append account.


### 6. Describe the following operation logic.
- Delete a record
1. Check the record's status, if it has been deleted we do nothing and return.
If it has been migrated, we follow the pointer go to the real position of the record.
2. We calculate the total length of records after the record we want to delete.
We set the data of the deleted record to 0 and copy the data of all of the records after the deleted one and move forward. 
Set the extra data to 0.
3. Recalcate and restore the offset of each record after the deleted record, the freebytes of the Page. 

- Update a record
1. Check the record's status, if it has been deleted we do nothing and return.
If it has been migrated, we follow the pointer go to the real position of the record.
2. Calcaulate the size of the new record through the recordDescriptor.
3. If the size of new record larger than the old one and the freebytes of this page is not enough to store the new record.
We migrate record. We call the insertRecord to insert the new Record and get the new RID, then we delete the old one.
Use a function to hash the new RID to make sure the slot num and page num will always less than -1. 
We store the hashed new RID to the original Slot of old record.
4. If the freebytes of this page is enough for the new record.
We compact or expand the record. Move the records after the updated one forward or back ward.
Recalcate and restore the offset of each record after the updated record, the freebytes of the Page. 
   
- Scan on normal records
Scan the table file sequentially until it reach the end of file.
If one record meet the condition, retrieve the data and the corresponding RID.

- Scan on deleted records
Skip the deleted records.


- Scan on updated records
Skip the tombstone record to avoid scan the same record more than one time. 
The migrated record will be scaned sequentially. 


### 7. Implementation Detail
- Other implementation details goes here.



### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)