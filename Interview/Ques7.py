# arr=[-1,3,-5,-6,7,8,-9,-5,4,5,7,9,-6,3]
# output [-1,-5,-6,-9,-5,-6,3,7,8,4,5,7,9,3]

arr = [-1, 3, -5, -6, 7, 8, -9, -5, 4, 5, 7, 9, -6, 3]

# Separate negatives and non-negatives, preserving order
negatives = [x for x in arr if x < 0]
non_negatives = [x for x in arr if x >= 0]

# Combine
result = negatives + non_negatives

print(result)
