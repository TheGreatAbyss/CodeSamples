"""
Below is a recursive algorithm for computing the optimal value of the knapsack problem.
The lecture shows how this would work using a double for loop over both i and w, filling in
the entire 2-D array of i X w.
The recursion version below doesn't try to compute every single cell in that 2-D array, but simply computes each
cell as needed.
The leaves (base cases) of this recursion tree is when the recursion is looking for items where i or w is <=0 and
returns the value of the single item.

Note that in order to run this algorithm you need to change the allowed recursion depth in bash using the command:
ulimit -s 60000
"""

import sys

items = []
seen = {}

with open("knapsack_big.txt") as knapsack:
    for line in knapsack:
        split = line.strip().split(" ")
        items.append(
            # [value_1] [weight_1]
            (int(split[0]), int(split[1]))
        )

# first item is Knapsack Size (W), Number of Items (N)
W = items[0][0]
N = items[0][1]

sys.setrecursionlimit(70000)


def score(i, w):

    if (i, w) in seen:
        return seen[(i, w)]

    # Falls off array of possibilities
    if i == 0:
        return 0

    # Value if i is not included
    excluded = score(i-1, w)

    # Value if i is included
    if w - items[i][1] <= 0:
        included = 0
    else:
        included = score(i-1, w - items[i][1]) + items[i][0]

    # print(str(i) + " " + str(w) + " " + str(excluded) + " " + str(included))

    value = max(included, excluded)
    seen[(i, w)] = value

    return value

value = score(N, W)
print(value)
