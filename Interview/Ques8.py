# List comprehension
# lst = [1,2,3,4,2,3,4,5,6]

# Original list
lst = [1, 2, 3, 4, 2, 3, 4, 5, 6]

# 1. Square of each element
squared = [x**2 for x in lst]
print("Squared:", squared)

# 2. Get only even numbers
evens = [x for x in lst if x % 2 == 0]
print("Even numbers:", evens)

# 3. Remove duplicates (unordered)
unique = list({x for x in lst})
print("Unique values (unordered):", unique)

# 4. Filter numbers greater than 3
greater_than_three = [x for x in lst if x > 3]
print("Numbers > 3:", greater_than_three)

# 5. Create a list of tuples (x, x^2)
tuple_list = [(x, x**2) for x in lst]
print("Tuples (x, x^2):", tuple_list)