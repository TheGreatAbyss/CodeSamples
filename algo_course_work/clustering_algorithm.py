# This is my implementation of a clustering algorithm in which k = 4.
# I am making use of the python_algorithm package's union_find
# Note I am subtracting 1 from the name of every node because the union_find package works by initializing a
# union find object with nodes named 0 to N.  So a 500 node graph would be initialized with 499

from union_find import UF
import heapq

edge_heap = []
set_of_nodes = set([])
k = 4

with open("clustering1.txt") as graph:
    for line in graph:
        split = line.strip().split(" ")
        heapq.heappush(edge_heap,
                       # Add a tuple in the form of (cost, (node1, node2))
                        (
                           int(split[2]), (int(split[0]) - 1, int(split[1]) - 1)
                        )
                       )

        # creating a set of nodes solely to get the node count later to initialize the union find object
        set_of_nodes.add(split[0])
        set_of_nodes.add(split[1])

union_find = UF(len(set_of_nodes))

# Keep popping the smallest edge off the heap
# if the nodes are not connected then union them
while union_find.count() > k:
    (cost, (node_1, node_2)) = heapq.heappop(edge_heap)
    if not union_find.connected(node_1, node_2):
        union_find.union(node_1, node_2)


# The question asks to find the maximum and minimum spacing after clustering
# the answer lies in the remaining edges in the heap.
# First build a node dictionary for each node pointing to what cluster it is in
# Also build a cluster dictionary that stores one value for each of N to N clusters
# Then keep popping from the heap, and store the edge cost in the cluster dictionary according to which cluster each
# of it's nodes are in
# Once the cluster dictionary is full, the largest value is the maximum spacing, and the smallest is the minimum

# Creating the node dictionary runs in O(n)
# Creating the cluster dictionary runs in less then O(k^2).
# Therefore if k is sufficiently smaller then n, this doesn't effect the overall running time of O(m|alpha(N))


node_to_cluster = {}
c = 0
for cluster in union_find.get_components():
    for node in cluster:
        node_to_cluster[node] = str(c)
    c+=1


cluster_dict = {}
for i in range(k):
    for j in range(i, k):
        if j > i:
            cluster_1 = str(i)
            cluster_2 = str(j)
            cluster_dict[cluster_1+"_"+cluster_2] = None

while None in cluster_dict.values():
    (cost, (node_1, node_2)) = heapq.heappop(edge_heap)
    cluster_1 = node_to_cluster[node_1]
    cluster_2 = node_to_cluster[node_2]

    if cluster_1 != cluster_2:
        if not cluster_dict.get(cluster_1+"_"+cluster_2):
            cluster_dict[cluster_1 + "_" + cluster_2] = cost

        if not cluster_dict.get(cluster_2+"_"+cluster_1):
            cluster_dict[cluster_2 + "_" + cluster_1] = cost

print("Max: " + str(max(cluster_dict.values())))
print("Min: " + str(min(cluster_dict.values())))  #  The course calls this the max
