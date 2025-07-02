# [1,10,3,3,4,5,10,1,1,3]
# Second largest number in python 
# What if we have 10 10s in the list
numbers = [1, 10, 3, 3, 4, 5, 10, 1, 1, 3]

# Remove duplicates
unique_numbers = list(set(numbers))

# Sort in ascending order
unique_numbers.sort()

# Get the second largest number
if len(unique_numbers) >= 2:
    second_largest = unique_numbers[-2]
    print("Second largest number is:", second_largest)
else:
    print("Not enough unique numbers to find second largest.")


