# neonpandas

A Pandas-centric interface to the Neo4j graph database.

![](resources/neonpandas_logo.png)

`import neonpandas as npd`

`pets = npd.NodeFrame(pets column='species', labels={'Pet'})`

Facilitates working with graph data with all the features of the Pandas/Numpy/Scipy stack.

This is accomplished by Node and Edge Frame classes, which are Pandas DataFrames with added constraints such as enforced _labels_ and _id_ columns that integrate with Neo4j.