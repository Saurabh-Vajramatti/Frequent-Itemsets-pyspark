# Frequent-Itemsets-pyspark
Both the codes are implementing SON Algorithm using Apache Spark Framework. The goal is to find all the possible combinations of the frequent item sets in any given input file within the required time.

<h3>FrequentBusinessUserSets.py:</h3>
<p>
  Here there are two cases:
  <ol>
    <li> Calculating the combinations of frequent businesses (as singletons, pairs, triples, etc.) that are qualified as frequent given a support threshold
    <li> Calculating the combinations of frequent users (as singletons, pairs, triples, etc.) that are qualified as frequent given a support threshold.
 </ol>
</p>

<p>
  Input format:   
  <ol>
    <li> Case number: Integer that specifies the case.
    <li> Support: Integer that defines the minimum count to qualify as a frequent itemset.
    <li> Input file path: has information on user, business, ratings, etc.
    <li> Output file path
 </ol>
</p>

<p>
  Output:   
  <ol>
    <li> Duration of execution
    <li> The frequent item set candidates and the final set of frequent item sets.
  </ol>
</p>

<img width="1000" src="https://user-images.githubusercontent.com/60020847/89115762-cc5f1400-d440-11ea-8dfc-4dd44306603f.png">

![image](https://user-images.githubusercontent.com/60020847/89115762-cc5f1400-d440-11ea-8dfc-4dd44306603f.png)
