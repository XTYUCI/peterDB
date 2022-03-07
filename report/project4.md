## Project 4 Report


### 1. Basic information
- Team #:
- Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-winter22-XTYUCI
- Student 1 UCI NetID: txie11
- Student 1 Name: Tianyi Xie
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).

I increase an Indexs table to store all catalog information about index.

| table_id | column_name  | inedex_file_name |

index file name is consist of table name + index attribute name +'.idx'

When we want to create an index on an attribute. The index file information will be add to the Indexs table.
Then we create the index file and scan all tuple of that table and insert the specific attribute into that index file.

When we insert a tuple into a table. We check if the table if any indexed attributes. If it does, we should insert all of the
index attributes from the new tuple into their corresponding index file. Delete tuple is the same logic, we delete the keys of
these attributes from the corresponding index file.

Destroy index file is just destroy the entire index file. 

### 3. Filter
- Describe how your filter works (especially, how you check the condition.)

Scan all tuples from the table and get the value of the target filter attribute from each tuple. Then we check if it is satisfied, if it does, we put the result into 
data and return.

### 4. Project
- Describe how your project works.

Scan all tuple from the table and get the value of the target project attribute from each tuple. Then we use them to create a new tuple with the null indcator, 
we put the result into data and return. 

### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)

Block buffer: numpages * PAGE_SIZE

We have an unorder map of { cmpareKey: <matched left tuple offset>,...... }

Load block: we scan the left table until all the table is scaned or the block buffer is full. Also, when we scan each tuple, we get the value of the compare key and
the tuple offset  from each tuple and update the map. 

Then we scan all the tuple in right table and merge the left tuple and right tuple whenever their compare key is matched. We put the merged result into data and
return.

When the right tablse end scan, we load next block from left table until we scaned all the left table. 

When left table scan end, we return QE_EOF. 

### 6. Index Nested Loop Join
- Describe how your index nested loop join works.

We scaned the left table.
For each left tuple, we serach the compare key in the index file of right table. When we find the same key, we merge the matched right and left tuple and put them into 
data and return.
When left table scan end, we return QE_EOF. 

### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).

Not applied.

### 8. Aggregation
- Describe how your basic aggregation works.

I set global values max,min, sum, count,avg.

Scaned all the tuple, and get the target value and updates these aggreate values.

When scan is done, we calculate the avg by sum/count.

We choose the result according to the op, add null indicator and put them into the data and return. 

If the table has been scaned with aggregation work, it can not be scaned again.

- Describe how your group-based aggregation works. (If you have implemented this feature)

Not applied.

### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.

No.

- Other implementation details:



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)