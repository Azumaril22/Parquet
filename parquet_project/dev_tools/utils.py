import functools
import time
from sys import stdout

# Définition des couleurs ANSI
COLORS = {
    "red": "\033[91m",
    "green": "\033[92m",
    "yellow": "\033[93m",
    "blue": "\033[94m",
    "reset": "\033[0m",  # Reset color
}


def style_func(text, color="reset"):
    return f"{COLORS[color]}{text}{COLORS['reset']}\r\n"


def print_color(text, color="reset"):
    stdout.write(style_func(text, color))


def timeit(func):
    """Décorateur pour mesurer le temps d'exécution d'une fonction."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()  # Démarre le chronomètre
        result = func(*args, **kwargs)
        end_time = time.perf_counter()  # Arrête le chronomètre
        execution_time = end_time - start_time
        print_color(
            f"{func.__name__} exécutée en {execution_time:.6f} secondes.", color="green"
        )
        return result

    return wrapper
