# anagaram

def is_anagram(str1, str2):
    # Remove spaces and convert to lowercase
    str1 = str1.replace(" ", "").lower()
    str2 = str2.replace(" ", "").lower()

    # Sort and compare
    return sorted(str1) == sorted(str2)

# Example usage
word1 = "listen"
word2 = "silent"

if is_anagram(word1, word2):
    print(f'"{word1}" and "{word2}" are anagrams.')
else:
    print(f'"{word1}" and "{word2}" are NOT anagrams.')
