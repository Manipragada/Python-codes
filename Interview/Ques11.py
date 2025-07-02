# NAVEEN
# how many times each character repeated

from collections import Counter

text = "NAVEEN"

char_counts = Counter(text)

for char, count in char_counts.items():
    print(f"'{char}' appears {count} times")