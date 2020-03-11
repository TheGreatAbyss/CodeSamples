"""
Below is my implementation of creating a leader board with dense ranking,
And a function to
  1) insert new scores into the leader board
  2) return what position each score will be in after each game

example of dense ranked leader board:
scores:              [100, 100, 90, 80, 70, 70, 60]
corresponding ranks: [1,   1,   2,  3,  4,  4, 5]

If leader board has m scores there's an initial linear cost to create the dense ranked leader board

Once created, each score being inserted into the leader board runs in log(m) time
- dominated by the binary search algorithm in bisect
Therefore sending a list of new scores with n scores runs in nlog(m) time

This is a similar solution to a question on Hacker Rank,
but the Hacker Rank question assumes the player scores are inserted in sorted order
Although the HR question can be answered in linear time, it's not really interesting or realistic.
I prefer the below solution which solves for any random input of new player scores.
"""


import bisect

# Subclass the builtin Python List and override basic methods
class DenseList(list):
    def __init__(self):
        super(DenseList, self).__init__()

    def insert(self, index, obj, count = 1):
        super(DenseList, self).insert(index, (obj, count))

    def append(self, obj, count = 1):
        super(DenseList, self).append((obj, count))

    def __getitem__(self, i):
        return super(DenseList, self).__getitem__(i)

    def __setitem__(self, i, obj, count = 1):
        super(DenseList, self).__setitem__(i, (obj, count))

    def increment_container(self, i):
        (obj, count) = self.__getitem__(i)
        count += 1
        self.__setitem__(i, obj, count)


def create_dense_list_leader_board(leader_board):
    leader_board.sort()  # Just in case not already sorted
    dense_list = DenseList()
    last_score = None
    for score in leader_board:
        if score == last_score:
            dense_list.increment_container(len(dense_list) - 1)
        else:
            dense_list.append(score)
        last_score = score
    return dense_list


def get_position_of_new_scores_into_leader_board(leader_board, new_scores):
    player_positions = []
    for score in new_scores:
        insertion_point = bisect.bisect_left(leader_board, (score, 1))
        if insertion_point == len(leader_board):
            leader_board.append(score)
            player_positions.append(1)
        elif leader_board[insertion_point][0] == score:
            leader_board.increment_container(insertion_point)
            player_positions.append(len(leader_board) - insertion_point)
        else:
            player_positions.append(len(leader_board) - insertion_point + 1)
            leader_board.insert(insertion_point, score)
    return player_positions, leader_board


def test_dense_list():
    dense_list = DenseList()
    dense_list.append(44)
    dense_list.append(55)
    dense_list.append(66)
    bisected = bisect.bisect_left(dense_list, (60, 0))
    dense_list.insert(bisected, 60)
    dense_list.increment_container(2)
    assert dense_list == [(44,1),(55,1),(60,2),(66,1)]


def test_get_position_of_new_scores_into_leader_board():
    leader_board = [900, 800, 700, 600, 500]
    my_scores = [750, 200, 1000, 600, 200]
    expected_positions = [3, 7, 1, 6, 8]
    expected_leader_board = [(200, 2), (500, 1), (600, 2), (700, 1), (750, 1), (800, 1), (900, 1), (1000, 1)]
    initial_dense_leader_board = create_dense_list_leader_board(leader_board)
    actual_positions, actual_leader_board = get_position_of_new_scores_into_leader_board(initial_dense_leader_board, my_scores)
    assert(expected_positions == actual_positions)
    assert (expected_leader_board == actual_leader_board)

