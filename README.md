# NeonPandas

A Pandas-centric interface to the Neo4j graph database.

![](src/logo.png)

Inspired by projects such as [geopandas](https://github.com/geopandas/geopandas), [huntlib](https://github.com/target/huntlib), and [cyberpandas](https://github.com/ContinuumIO/cyberpandas), `neonpandas` facilitates working with Neo4j  graph data with all of the features of the Pandas/NumPy/SciPy stack.

This is accomplished by NodeFrames and EdgeFrames, which are Pandas DataFrames with added constraints such as enforced _labels_ and _id_ columns that ensure proper integration with Neo4j.


## Example Usage
`import neonpandas as npd`

`data = [{'name': 'Ralph', 'species': 'Dog', 'age': 10}, {'name': 'Pip', 'species': 'Cat', 'age': 7}, {'name': 'Babe', 'species': 'Pig', 'age': 3}]`

`pets = npd.NodeFrame(data, column='species', labels={'Pet'})`