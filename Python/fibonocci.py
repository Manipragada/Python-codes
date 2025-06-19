# Fibonacci series up to n terms

def fibonacci_series(n):
    a, b = 0, 1
    series = []
    for _ in range(n):
        series.append(a)
        a, b = b, a + b
    return series

# Example usage
n_terms = 10
print(f"Fibonacci series up to {n_terms} terms:")
print(fibonacci_series(n_terms))
