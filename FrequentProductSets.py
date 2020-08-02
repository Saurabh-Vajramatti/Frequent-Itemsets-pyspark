import pyspark
from pyspark import SparkConf, SparkContext
import sys
from itertools import combinations
import json
from collections import OrderedDict


import time

def chunk_function(partition):
    partition_list=[]
    for bas in partition:
        partition_list.append(bas)

    number_of_baskets_in_partition=len(partition_list)
    partition_support=support*(number_of_baskets_in_partition/number_of_baskets)

    count=dict()
    final_frequent_list=[]
    frequent_singleton_set=set()
    candidate_singleton_set=set()
    for basket in partition_list:
        for item in basket[1]:
            candidate_singleton_set.add(item)
            if item in count:
                count[item]=count[item]+1
            else:
                count[item]=1
            if count[item]>=partition_support:
                frequent_singleton_set.add(item)
    final_frequent_list=sorted(list(frequent_singleton_set))



    #For size 2
    previous_frequent_list=list(frequent_singleton_set)
    current_candidates_list=[]
    current_frequents_list=[]
    current_count={}
    for candidate_doublet in combinations(previous_frequent_list,2):
        current_candidates_list.append(tuple(sorted(candidate_doublet)))

    for basket in partition_list:
        for candidate in current_candidates_list:
            if(set(candidate).issubset(set(basket[1]))):
                if(candidate in current_count):
                    current_count[candidate]+=1
                else:
                    current_count[candidate]=1
                if(current_count[candidate]>=partition_support):
                    current_frequents_list.append(candidate)

    for item in current_frequents_list:
        final_frequent_list.append(item)


    #For all others>=3
    current_candidate_size = 3
    while(True):
        # print("SIZE=", current_candidate_size)
        previous_frequent_list = current_frequents_list
        current_candidates_list = []
        current_frequents_list = []
        current_count = {}
        for candidate_combination in combinations(previous_frequent_list,2):
            if(len(set(candidate_combination[0]).union(set(candidate_combination[1])))==current_candidate_size):
                possible_combination=tuple(sorted(set(candidate_combination[0]).union(set(candidate_combination[1]))))
                if(possible_combination in current_candidates_list):
                    continue
                immediate_subsets = combinations(possible_combination, current_candidate_size - 1)
                if set(immediate_subsets).issubset(previous_frequent_list):
                    current_candidates_list.append(possible_combination)


        for basket in partition_list:
            for candidate in current_candidates_list:
                if set(candidate).issubset(set(basket[1])):
                    if (candidate in current_count):
                        current_count[candidate] += 1
                    else:
                        current_count[candidate] = 1
                    if (current_count[candidate] >= partition_support):
                        current_frequents_list.append(candidate)

        if(len(current_frequents_list)==0):
            break
        for item in current_frequents_list:
            final_frequent_list.append(item)

        current_candidate_size+=1

    return final_frequent_list

def calculate_support(partition):
    frequency={}
    for basket in partition:
        for candidate in candidate_list:
            # print(type(candidate),candidate)
            if(type(candidate) is int):
                if({candidate}.issubset(set(basket[1]))):
                    if(candidate in frequency):
                        frequency[candidate]+=1
                    else:
                        frequency[candidate]=1
            else:
                if(set(candidate).issubset(set(basket[1]))):
                    if (candidate in frequency):
                        frequency[candidate] += 1
                    else:
                        frequency[candidate] =1
    return_list=[]
    for key in frequency:
        return_list.append((key,frequency[key]))

    return return_list


start = time.time()

sc = SparkContext(conf=SparkConf().setAppName("task1").setMaster("local[*]"))
sc.setLogLevel("OFF")

filter_threashold=int(sys.argv[1])
support=float(sys.argv[2])
input_file_path=sys.argv[3]
output_file_path=sys.argv[4]

file_rdd=sc.textFile(input_file_path)

head=file_rdd.first()
file_rdd=file_rdd.filter(lambda l:l!=head)\
    .map(lambda line: line.split(","))

# print(file_rdd.first())

# file_rdd=file_rdd.collect()
# print(file_rdd)

output_file=open(output_file_path,"w")


# Data processing to get userID-Puechase Date as key
rdd = file_rdd.map(lambda line: (line[0][1:-1]+"-"+line[1][1:-1], {int(line[5][1:-1])}))

# print(rdd.first())

# Filtering out the users who made less purchases
user_baskets = rdd.reduceByKey(lambda a, b: a.union(b)).filter(lambda l: len(l[1])>filter_threashold)

# print(user_baskets.first())
#print(user_baskets.sortByKey().collect())

#Starting SON
# Creating the user basket
son=user_baskets.map(lambda l: (l[0],l[1]))
number_of_baskets = user_baskets.count()
#son.persist()
candidate_list=son.mapPartitions(chunk_function).collect()
candidate_list=list(set(candidate_list))

candidates_dictionary={}
frequents_dctionary={}
for candidate in candidate_list:
    if(type(candidate) is int):
        if(1 in candidates_dictionary):
            candidates_dictionary[1].append(candidate)
        else:
            candidates_dictionary[1]=[]
            candidates_dictionary[1].append(candidate)

    else:
        if(len(candidate) in candidates_dictionary):
            candidates_dictionary[len(candidate)].append(candidate)
        else:
            candidates_dictionary[len(candidate)]=[]
            candidates_dictionary[len(candidate)].append(candidate)

output_file.write("Candidates:")
# Writing candidates to file
candidate_size=1
while(True):
    if candidate_size in candidates_dictionary:
        output_file.write("\n")
        candidates_dictionary[candidate_size]=sorted(candidates_dictionary[candidate_size])
        for candidate in candidates_dictionary[candidate_size]:
            if(type(candidate) is int):
                output_file.write("('"+str(candidate)+"')")
                if(candidate!=candidates_dictionary[candidate_size][-1]):
                    output_file.write(",")
                else:
                    output_file.write("\n")

            else:
                output_file.write("(")
                for item in candidate:
                    if(item!=candidate[0]):
                        output_file.write(" ")
                    output_file.write("'"+str(item)+"'")
                    if(item!=candidate[-1]):
                        output_file.write(",")
                output_file.write(")")
                if (candidate != candidates_dictionary[candidate_size][-1]):
                    output_file.write(",")
                else:
                    output_file.write("\n")
        candidate_size+=1
    else:
        break

# print("candidates above")
# print(candidate_list)

#Phase 2 getting frequent sets

# common_candidate_list=sc.broadcast(candidate_list)
# print("BROADCAST",common_candidate_list.value)
item_support=son.mapPartitions(calculate_support)
#The critical support check
frequent_list=item_support.reduceByKey(lambda a,b: a+b).filter(lambda l:l[1]>=support).collect()

for candidate in frequent_list:
    if (type(candidate[0]) is int):
        if (1 in frequents_dctionary):
            frequents_dctionary[1].append(candidate[0])
        else:
            frequents_dctionary[1] = []
            frequents_dctionary[1].append(candidate[0])

    else:
        if (len(candidate[0]) in frequents_dctionary):
            frequents_dctionary[len(candidate[0])].append(candidate[0])
        else:
            frequents_dctionary[len(candidate[0])] = []
            frequents_dctionary[len(candidate[0])].append(candidate[0])

output_file.write("\nFrequent Itemsets:")
# writing frequent itemsets to file
candidate_size = 1
while (True):
    if candidate_size in frequents_dctionary:
        if(candidate_size==1):
            output_file.write("\n")
        else:
            output_file.write("\n\n")
        frequents_dctionary[candidate_size] = sorted(frequents_dctionary[candidate_size])
        for candidate in frequents_dctionary[candidate_size]:
            if (type(candidate) is int):
                output_file.write("('" +str(candidate)+ "')")
                if (candidate != frequents_dctionary[candidate_size][-1]):
                    output_file.write(",")

            else:
                output_file.write("(")
                for item in candidate:
                    if (item != candidate[0]):
                        output_file.write(" ")
                    output_file.write("'" +str(item)+ "'")
                    if (item != candidate[-1]):
                        output_file.write(",")
                output_file.write(")")
                if (candidate != frequents_dctionary[candidate_size][-1]):
                    output_file.write(",")
        candidate_size += 1
    else:
        break



#user_basket_rdd= file_rdd.reduceByKey(lambda a,b: ( a.union(b) if(type(b) is set) else (a.union({b}))) if(type(a) is set)  else ({a}.union(b) if(type(b) is set) else({a}.union({b}) ))).collect()
#print(user_basket_rdd)


print("Duration:",(time.time() - start))