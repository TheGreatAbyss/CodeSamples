"""
Below is the Floyd Warshall algorithm for computing the shortest path between all nodes in a directed graph
The graph can have negative edges, but if it has a negative cycle the algo will stop.
This runs in O(n^3) time
"""

import numpy as np
from collections import defaultdict

node_dict = defaultdict(dict)
distinct_nodes_found = set([])

"""
Example of node_dict

{   node 1 : {
            node x: cost,
            node y: cost
            }
    node 2 :{
            node x: cost,
            node y: cost
            }
}
"""

with open("g_test.txt") as f:
    for line in f:
        (tail, head, cost) = line.strip().split(" ")
        node_dict[tail][head] = cost
        distinct_nodes_found.add(tail)
        distinct_nodes_found.add(head)

n = len(distinct_nodes_found)
# Note I am adding 1 to k, i and j because numpy starts indexing arrays at zero
value_array = np.zeros((n + 1, n + 1, n + 1))
value_array.fill(float('inf'))

# pre-fill step for k = 0, Set Cij for all nodes that are next to each other and are easily known
# just from reading the graph data.
# runs in O(m) time
for tail, head_dict in node_dict.items():
    for head, cost in head_dict.items():
        value_array[0, head, tail] = cost

# Main loop of dynamic programing algorithm
# Each loop through k tries seeing if there is a shorter path from i to j with k in the middle.
# If we know the shortest path subproblem from i -> k and k -> j, then we know the shortest path i -> j
# is the minimum of either the previous computed shortest path, or i -> k + i ->j
for k in range(1, n + 1, 1):
    for i in range(1, n + 1, 1):
        for j in range(1, n + 1, 1):
            shortest_path = min(value_array[k-1, i, j], value_array[k-1, i, k] + value_array[k-1, k, j])
            value_array[k, i, j] = shortest_path

    # If there is a negative cost cycle, then a value will show a negative cost getting back to itself
    # If this is seen on any iteration of k then stop the loop and exit the program
    if np.min(np.diag(value_array[k])) < 0:
        print("negative cost cycle found")
        print(value_array[k])
        for index, item in enumerate(np.diag(value_array[k])):
            if item < 0:
                print("index: " + str(index) + " item: " + str(item))
        quit()

# print all shortest paths
print(value_array[k])
print("the smallest shortest path ")
# I'm assuming i -> i paths count?? The quiz wasn't clear
print(np.min(value_array[k]))
