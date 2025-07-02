# Get even from the list and sum of squares of even number in python
# List(1,2,3,4,5,6)

numbers = [1, 2, 3, 4, 5, 6]

# Get even numbers
even_numbers = [n for n in numbers if n % 2 == 0]

# Sum of squares of even numbers
sum_of_squares = sum(n ** 2 for n in even_numbers)

print("Even numbers:", even_numbers)
print("Sum of squares of even numbers:", sum_of_squares)
