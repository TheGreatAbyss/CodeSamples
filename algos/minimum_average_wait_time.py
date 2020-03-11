import heapq

"""
Greedy Algorithm for computing minimum average wait time assuming a list of customers coming at time t, with a cook_time
Optimizes by running the shortest cook_time first
Basically the same thing as the scheduling greedy algo except value is always the same.
"""

def minimum_average_wait_time(customers):
    customers = sorted(customers)
    t = 0
    h = []
    customer_sum = 0
    customer_count = 0

    while customers or h:
        # Add any customers who have arrived since last cook
        while customers and customers[0][0] <= t:
            (arrival_time, cook_time) = customers.pop(0)
            customer_count += 1
            heapq.heappush(h, (cook_time, arrival_time))

        if h:
            # print(customers)
            (cook_time, arrival_time) = heapq.heappop(h)
            # print(cook_time)
            customer_sum += (t + cook_time - arrival_time)
            t += cook_time
        else:
            # Handle in case all customers have been served but more haven't arrived yet
            t = customers[0][0]

    return customer_sum // customer_count


def test_cast_one():
    customers = [
        (0, 3),
        (1, 9),
        (2, 5)
    ]

    expected = 8
    actual = minimum_average_wait_time(customers)
    print(actual)
    assert expected == actual


test_cast_one()


def test_case_two():
    expected = 16673945929151
    customers = []
    with open("/Users/ericabis/dumps/hacker_rank/minimum_average_waiting_time_test_case.txt", 'r') as fh:
        fh.readline()
        for line in fh.readlines():
            line = line.strip("\n")
            customers.append(tuple(map(int, line.split(" "))))
    print(customers[:4])
    actual = minimum_average_wait_time(customers)
    print (actual)
    assert expected == actual


test_case_two()
