## Project 1 Report


### 1. Basic information
 - Team #:  
 - Github Repo Link:   https://github.com/UCI-Chenli-teaching/cs222-winter22-XTYUCI.git
 - Student 1 UCI NetID: 76761561
 - Student 1 Name: TianyiXie
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Internal Record Format
- Show your record format design.
	
| null indicator | num of fields | filed1offset |filed2 offset | .... | record data |
			
	
Describe how you store a null field.

check the null indicator by the number position of the field. 

Describe how you store a VarChar field.

store the varChar field offset and its length.  

Describe how your record design satisfies O(1) field access.

get the field offset through the position number of field among all fields.

use recordoffset+fieldoffset to access the field. 

### 3. Page Format
- Show your page format design.

The record is insert fron the begining of a page. 
From the end of a page, there is 2 bytes recording the freebytes of that page, 2 bytes recording the number of records, and 4 bytes of each slot with corresponing records.

- Explain your slot directory design if applicable.

2 bytes for the record pointer offset which points to the begining of each record.
2 bytes for the record size.

### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.

If 0 page in the file then we apend a new page and insert the record.
Checking the free space of the last exist page if it has free spcae then we insert the record.
Iterate all exist page from the first page. if any one of them has free space then we insert the record.
No exist page has free space then we append a new a page and insert. 	

- How many hidden pages are utilized in your design?

One, the hidden page is created with the file.

- Show your hidden page(s) format design if applicable

It store the number of pages, append counter, read counter and the write counter. 

### 5. Implementation Detail
- Other implementation details goes here.



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)