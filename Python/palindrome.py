def is_palindrome(s):
    s = str(s)  # ensure it's a string
    return s == s[::-1]

# Example usage
word = "madam"
if is_palindrome(word):
    print(f"'{word}' is a palindrome.")
else:
    print(f"'{word}' is not a palindrome.")