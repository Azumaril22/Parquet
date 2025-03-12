import time
import functools


def timeit(func):
    """Décorateur pour mesurer le temps d'exécution d'une fonction."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()  # Démarre le chronomètre
        result = func(*args, **kwargs)
        end_time = time.perf_counter()  # Arrête le chronomètre
        execution_time = end_time - start_time
        print(f"{func.__name__} exécutée en {execution_time:.6f} secondes")
        return result
    return wrapper
