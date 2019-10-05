def merge_sort_and_inversions(arr, length):
    if length == 1:
        return arr, 0
    if length > 1:
        mid = length // 2
        left, left_inversions = merge_sort_and_inversions(arr[:mid], mid)
        right, right_inversions = merge_sort_and_inversions(arr[mid:], length - mid)

        return_array = []
        l = 0
        r = 0
        inversions = 0

        while l < mid or r < length - mid:
            if not left:
                return_array.append(right.pop(0))
                r += 1
            elif not right:
                return_array.append(left.pop(0))
                l += 1
            elif left[0] <= right[0]:
                return_array.append(left.pop(0))
                l += 1
            else:
                inversions += len(left)
                return_array.append(right.pop(0))
                r += 1

        # print(return_array)
        return return_array, inversions + right_inversions + left_inversions


tst_list = [4,5,2,7,9,3,12,3,5,7,8,9,6,3,2,5,7,9,0,3,2,21]
print(len(tst_list))
sorted_list, first_inversions = merge_sort_and_inversions(tst_list, len(tst_list))
print(sorted_list)
print(len(sorted_list))
print("inversions_1: {i}".format(i=first_inversions))
assert(len(tst_list) == len(sorted_list))


list_inversions = [1, 3, 5, 6, 2, 4, 6]
"""
Inversions should be:
(3,2), (5,2), (5,4), (6,2), (6,4)
"""
sorted_inversions, tst_inversions = merge_sort_and_inversions(list_inversions, len(list_inversions))
print(sorted_inversions)
print("inversions test: {0}".format(tst_inversions))
assert(tst_inversions == 5)