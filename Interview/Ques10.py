# list lo sum of consecutive numbers


lst = [1, 2, 3, 4, 5]

# Sum of consecutive pairs
consecutive_sums = [lst[i] + lst[i + 1] for i in range(len(lst) - 1)]

print(consecutive_sums)

