import duckdb
import re


def detect_data_type(value):
    """
    Détecte le type de données d'une valeur en Python, y compris les notations scientifiques.

    Args:
        value (str): La valeur à analyser (souvent sous forme de string)

    Returns:
        type: Le type détecté (int, float, bool, None, str)
    """

    if isinstance(value, bool):  # Vérifier si c'est déjà un booléen
        return bool
    elif isinstance(value, int):  # Vérifier si c'est déjà un entier
        return int
    elif isinstance(value, float):  # Vérifier si c'est déjà un flottant
        return float
    elif value is None or value in {"None", "null", "NULL"}:  # Gérer les valeurs nulles
        return type(None)
    elif isinstance(value, str):
        value = value.strip()

        # Vérifier si c'est un entier
        if re.fullmatch(r"-?\d+", value):
            return int

        # Vérifier si c'est un flottant (inclut la notation scientifique)
        if re.fullmatch(r"-?\d+\.\d+([eE][-+]?\d+)?", value) or re.fullmatch(r"-?\d+[eE][-+]?\d+", value):
            return float

        # Vérifier si c'est un booléen
        if value.lower() in {"true", "false"}:
            return bool

    return str


def get_list_dtype(values):
    """Détecte le type dominant dans une liste de tuples de valeurs."""
    if not values:
        return "Empty"  # Liste vide

    dtype_is_bool = True
    for value in values:
        if str(value).lower() not in ("true", "false", "0", "1"):
            dtype_is_bool = False
            break

    if dtype_is_bool:
        return "Boolean"

    detected_types = set()
    for value in values:
        dtype = detect_data_type(value[0])
        if dtype == str:
            # print("Value:", value[0])
            return "String"

        detected_types.add(dtype)

    priority = [float, int]

    for dtype in priority:
        if dtype in detected_types:
            if dtype == float:
                return "Float"
            elif dtype == int:
                return "Integer"

    return "String"


def get_column_dtype(datatable, col, dtype=None):
    detected_type = ""
    if dtype != "VARCHAR":
        if dtype in ("FLOAT", "REAL", "DOUBLE", "DECIMAL"):
            detected_type = "Float"
        elif dtype in ("BIGINT", "SMALLINT", "TINYINT"):
            detected_type = "Integer"
    else:
        query = f"""
            SELECT DISTINCT "{col}"
            FROM '{datatable}'
            WHERE TRIM("{col}") <> '.'
            ORDER BY "{col}" DESC
        """
        try:
            liste = duckdb.sql(query).fetchall()
            detected_type = get_list_dtype(liste)
        except Exception as e:
            print(query)
            raise e

    return detected_type
