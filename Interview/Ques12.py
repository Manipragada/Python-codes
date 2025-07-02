# hi hello hi hello hello hi hi 
# How many times hi repeated and hello repeated

text = "hi hello hi hello hello hi hi"

# Split text into words
words = text.split()

# Count occurrences
hi_count = words.count("hi")
hello_count = words.count("hello")

print(f"'hi' repeated {hi_count} times")
print(f"'hello' repeated {hello_count} times")
