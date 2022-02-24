## Project 3 Report


### 1. Basic information
 - Team #:
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-winter22-XTYUCI
 - Student 1 UCI NetID: txie11
 - Student 1 Name: TianyiXie
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 
   page 0 contains the pointer of the root page
   a hidden pagen contain the written,append and read counter.    

### 3. Index Entry Format
- Show your index entry design (structure). 
 a varchar key contains varchar length and vachar itself.
a int/real type key contains its value with 4 bytes.

  - entries on internal nodes:  
 |pointer|key|pointer|key|... 

  - entries on leaf nodes:
|key|RID|key|RID|...


### 4. Page Format
- Show your internal-page (non-leaf node) design.
 |pointer|key|pointer|key|...|key|pointer| .....(FreeBytes)..... | leafNodeChecker |keyNums |freebytes| 

leafNodeChecker=0 means its an interior page.
keyNums is the total number of key in this page

- Show your leaf-page (leaf node) design.

|key|RID|key|...|key|RID| .....(FreeBytes).....| nextPagePointer | leafNodeChecker |keyNums |freebytes| 
leafNodeChecker=1 means its an interior page.
keyNums is the total number of key in this page
nextPagePointer contains the pointer that pointer to next page, if its -1 means its the last leaf page and point to nothing.

### 5. Describe the following operation logic.
- Split

If the freebytes in is not enough then we do a split.
First we need to check the insert key will be insert in the original page or in the split page.
Then we need copy the split data to a new data buffer and insert the key to one of the split page or the original page.
If it is a split of interior nodes, we pick and remove the first key from the split page. Set it as the new parent key of the split page and the original page.
If it is a split of leaf nodes, we pick but not remove the first key from the split page. Set it as the new parent key of the split page and the original page.
If there is no parent interior page, we append a new one.
If there the parent interior page is also full, we then split the parent interior page. 
This whole insert process is doing recursively from buttom to up until the we reach the top layer in the tree.

- Rotation (if applicable)



- Merge/non-lazy deletion (if applicable)



- Duplicate key span in a page

For the duplicate key, they are stored in ascending order together.
  | key1| rid1 | key1| rid2 | key1| rid3 |..........

- Duplicate key span multiple pages (if applicable)



### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.



- Other implementation details:

To search the correct insert index, I use the binary search to reduce time complexity.
However, for the varchar type, its not avaliable to use the binary search because the length of each key is different.

### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
