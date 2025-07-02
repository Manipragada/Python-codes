# lst = List(1,2,3,4,5,6)
# get even from list and sum of squares of even numbers in python

lst = [1, 2, 3, 4, 5, 6]
# Get even numbers
even_numbers = [x for x in lst if x % 2 == 0]

# Compute sum of squares of even numbers
sum_of_squares = sum(x ** 2 for x in even_numbers)

print("Even numbers:", even_numbers)
print("Sum of squares of even numbers:", sum_of_squares)
