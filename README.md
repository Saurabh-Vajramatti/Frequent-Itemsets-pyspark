# Frequent-Itemsets-pyspark
Both the codes are implementing SON Algorithm using Apache Spark Framework. The goal is to find all the possible combinations of the frequent item sets in any given input file within the required time.

FrequentBusinessUserSets.py:
Here there are two cases:
1.	Calculating the combinations of frequent businesses (as singletons, pairs, triples, etc.) that are qualified as frequent given a support threshold.
2.	Calculating the combinations of frequent users (as singletons, pairs, triples, etc.) that are qualified as frequent given a support threshold
Input format: 
1.	Case number: Integer that specifies the case.
2.	Support: Integer that defines the minimum count to qualify as a frequent itemset. 
3.	Input file path: has information on user, business, ratings, etc.
4.	Output file path

Output:
1.	Duration of execution
2.	The frequent item set candidates and the final set of frequent item sets.
