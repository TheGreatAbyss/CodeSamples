import collections
import heapq

"""
The below algorithm is my implementation of Prim's Minimum Spanning Tree for the Coursera course, Algorithms, Design,
and Analysis part 2, Programming Assignment 1, question 3

The theoretical implementation of this algorithm is suppose to run in O[mlog(n)], but because Python's heapq object
does not support deletions, I came up with my own implementation that runs in O[mlog(m)].

In the theoretical implementation, the algorithm is supposed to delete all but the minimum edge incident on a node
in the unexplored cut that crosses into the explored cut.  Because I can't do this in Python I simply don't delete
these edges.  In order to account for this, I keep popping the min element from the heap until I find an edge
leading to an unexplored verticy and throwing away edges pointing to vertices that are already explored.
This has the effect of taking my heap size from the theoretical size which should be the number of vertices,
to the number of edges

Input data is of the format: "Node 1, Node 2, Cost"

Create a master dictionary for the graph with the keys being nodes, and the values being a sub dictionary
where the keys are connected nodes, and the values being the edge cost.
ex Node 1 connects to node 2 with a cost of 50, and to node 3 with a cost of 25 {1: {2: 50, 3: 25}}
"""


master = collections.defaultdict(lambda: collections.defaultdict(int))

"""
@type master: dict[int, dict[int,str]]'
"""

# Create a set of all nodes, and seen_nodes
all_nodes = set([])
found_nodes = set([])

with open("prims_minimum_spanning_tree_edges.txt") as f:
    for line in f:
        cleaned = line.strip()
        line_array = cleaned.split(' ')
        node_1 = int(line_array[0])
        node_2 = int(line_array[1])
        cost = int(line_array[2])

        # In case two vertices have more then one connecting edge, this takes the minimum
        if master[node_1][node_2]:
            existing_value = master[node_1][node_2]
            new_cost = min(existing_value, cost)
        else:
            new_cost = cost

        master[node_1][node_2] = new_cost
        master[node_2][node_1] = new_cost

        all_nodes.add(node_1)
        all_nodes.add(node_2)


# Start on the first node in the all_nodes set
first_elem = all_nodes.pop()
found_nodes.add(first_elem)

"""
Build a Heap with indexes being the cost of connecting to the first_elem,
example node 2 connects to first_elm with a cost of -544: (-544, 2)
"""
heap = [(a[1], a[0]) for a in master[first_elem].items()]
heapq.heapify(heap)

total_cost = 0


def pull_off_heap():
    global heap
    global found_nodes
    global total_cost
    global master

    # Keep pulling elements off the heap until you find one we haven't seen before, or heap is empty
    # Arbitrarily set next_item to the first_element pulled as we know we've seen it before, insuring the loop continues
    next_item = (99, first_elem)

    while next_item[1] in found_nodes and heap:
        next_item = heapq.heappop(heap)

    # If the heap is empty then the MST search is complete
    if not heap:
        return

    # Add in new vertices from next_item to heap only if they are unexplored
    for node, score in master[next_item[1]].items():
        if node not in found_nodes:
            heapq.heappush(heap, (score, node))

    found_nodes.add(next_item[1])
    total_cost = total_cost + next_item[0]

while heap:
    pull_off_heap()

print(total_cost)
