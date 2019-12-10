"""
The below algorithm computes the solution to the Traveling Salesman problem with nodes existing in Euclidean (x,y) space
This is an NP Complete problem and is solved in O(n^2 * 2^n) time.
[2^n choices of S] X [n choices of j] X [n work per subproblem]
"""


import itertools
import gc

from collections import defaultdict


node_dict = {}
"""
Format
{node1 : [x, y],
 node2 : [x, y]
 ...
 }
"""
nodes = []

node_num = 0
with open("tsp_test.txt") as f:
    for line in f:
        (x, y) = line.strip().split(" ")
        node_dict[node_num] = (float(x), float(y))
        nodes.append(node_num)
        node_num += 1


def compute_distance(node1, node2):
    (x, y) = node_dict[node1]
    (z, w) = node_dict[node2]
    return ((x-z)**2.0+(y-w)**2.0)**0.5


# Precompute node differences to save computation time.  Only n^2 combinations which is irrelevant given the far larger
# exponential running time of the main loop of the algorithm
node_distances = defaultdict(dict)
"""
Format:
{n1 : {n2: v,
       n3: v,
       ...}
"""
for n1 in nodes:
    for n2 in nodes:
        node_distances[n1][n2] = compute_distance(n1, n2)

infinity = float('inf')

# In this implementation I will be using a regular python dictionary (hash map) in order to preserve memory,
# instead of a full 2-D array using a tool such as Pandas.
# The problem with the full 2-D array implantation is that most values are never a viable combination of S and j
# For example the S = {0,1,2} will only have j in [1 ,2].  In a 2-D array, j [3 -> 10] will always be empty.

# To preserve memory I will only be storing the values computed at each previous iteration of m.

m_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: int)))
"""
Format:
m1: {
        {S1 : {j1 : value1,
               j2 : value2,
                ...},
         {S2 : {j1 : value1,
                j2 : value2,
                ...},
    },
m2: {
        {S1 : {j1 : value1,
               j2 : value2,
                ...},
         {S2 : {j1 : value1,
                j2 : value2,
                ...},
    },
    ....
}
"""

# The base case of the dictionary is where S and j are 0 which gives a path of 0.
m_dict[1]['[0]'][0] = 0

# The recurrence function computes the shortest tsp distance to get from node 0 to node j given a set of nodes S
# The results are stored in the dictionary as the subproblems to larger and larger tsp paths.
# The path to j can use any node k as it's penultimate vertex, therefore the recurrence takes the min of all possible
# tsp paths ending in k + the final hop from k to j.
# Each of these smaller tsp paths will already have been solved by early iterations of dynamic programming loop


def recurrence(S, j):
    s_without_j = S.copy()
    s_without_j.remove(j)
    str_set_without_j = str(s_without_j)

    min_value = float('inf')
    for k in S:
        min_value = min(min_value, m_dict[m-1].get(str_set_without_j, {}).get(k, infinity) + node_distances[k][j])

    return min_value


# Main loop of the algorithm

# Outer loop that runs through sub problem sizes from 2 to n
for m in range(2, len(nodes) + 1):
    print(m)
    count_of_combos = 0

    # A generator that produces every possible combination of nodes of size m (nCm)
    combinations_generator = itertools.combinations(nodes, m)

    # A generator that sits on top of the above generator that filters for only combos that contain node 0
    set_combinations_generator = (list(combo) for combo in combinations_generator if 0 in combo)

    # first inner loop that iterates through each combination of nodes of size m that contains node 0
    for set_combination in set_combinations_generator:

        count_of_combos += 1
        if count_of_combos % 10000 == 0:
            print(count_of_combos)

        combination_str = str(set_combination)

        j_generator = (j for j in set_combination if j != 0)

        # second inner loop that iterators through each value j of each combination where j is not 0
        for j in j_generator:
            sub_problem_value = recurrence(set_combination, j)
            m_dict[m][combination_str][j] = sub_problem_value

    # In order to preserve memory delete everything smaller then m
    # This keeps sets of only m size of data and looses everything smaller
    # This is possible because all future bigger sub problems will never have to look at subproblems smaller then m

    m_dict[m-1] = {}
    gc.collect()

# Once the above loops are completed, we will know the smallest possible TSP path for each full set of 0 - through n
# with each vertex j of 1 through n being the final hop.
# In order to complete the final TSP path we need to find the min of the above paths plus their corresponding final hops
# From j to 0

str_full_set = str(nodes)
final_value = float('inf')
for j in range(1, len(nodes)):
    final_value = min(final_value, m_dict[m][str_full_set][j] + compute_distance(j, 0))

print("TSP Cost: " + str(final_value))

