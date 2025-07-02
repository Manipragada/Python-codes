# Get even from the list and sum of squares of even number in python
# List(1,2,3,4,5,6)
# Given list
numbers = [1, 2, 3, 4, 5, 6]
even_numbers = [num for num in numbers if num % 2 == 0]
sum_of_squares = sum(num ** 2 for num in even_numbers)
print("Even numbers:", even_numbers)
print("Sum of squares:", sum_of_squares)


