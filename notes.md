# Different ways to interact with Frame Classes

## 1. Separate Node & Edge lists

### a. Node (1) & Edge (1) lists contain uuuid

### b. Multiple Node lists and 1 Edge lists 
    - Each NodeFrame contains one _class_ of nodes (e.g. Pets, Owners, etc.)
    - Master EdgeFrame contains ways to link back to nodes
        - Via uuid
        - Via id-col & labels

## 2. Single Edgelist 
    - E