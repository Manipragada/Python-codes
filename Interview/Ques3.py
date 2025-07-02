# Write a higher order func that prints the given string given number of times
# Python
def repeat_action(action):
    def wrapper(string, times):
        for _ in range(times):
            action(string)
    return wrapper
def print_message(msg):
    print(msg)
repeat_print = repeat_action(print_message)
repeat_print("Hello, Python!", 3)
