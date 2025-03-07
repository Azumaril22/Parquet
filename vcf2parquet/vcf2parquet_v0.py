import polars as pl
import hashlib
from typing import Set
from collections import OrderedDict

# -------------------
# Étape 1 - Lecture échantillon et extraction des clés INFO
# -------------------

def extract_info_keys(sample_df: pl.DataFrame) -> Set[str]:
    """
    Parcourt la colonne INFO et extrait toutes les clés présentes (avant les "=").
    Retourne un set de clés uniques.
    """
    keys = set()

    for info in sample_df["INFO"]:
        if isinstance(info, str):  # Certaines lignes peuvent être vides
            for pair in info.split(";"):
                if "=" in pair:
                    key = pair.split("=", 1)[0]
                    keys.add(key)

    return keys

def extract_info_keys_preserve_order(sample_df: pl.DataFrame):
    keys = OrderedDict()  # Conserve l'ordre d'apparition (première apparition par ligne)
    for info in sample_df["INFO"]:
        if isinstance(info, str):
            for pair in info.split(";"):
                if "=" in pair:
                    key = pair.split("=", 1)[0]
                    if key not in keys:
                        keys[key] = True  # Premier passage uniquement
    return list(keys.keys())  # Retourne une liste ordonnée

# Lecture d'un échantillon en eager pour récupérer les clés INFO
sample_df = pl.read_csv(
    "Datasets/VCF_annovar.vcf",
    skip_rows=444,
    separator="\t",
    infer_schema_length=1000  # Limite pour ne pas parser tout le fichier
).head(10000)

info_keys = extract_info_keys_preserve_order(sample_df)
print(f"Clés détectées dans INFO: {sorted(info_keys)}")


# -------------------
# Étape 2 - Création du LazyFrame complet avec parsing dynamique
# -------------------

# Fonction pour extraire la valeur d'une clé spécifique dans INFO (en Lazy)
def extract_key(key: str):
    return (
        pl.col("INFO")
        .str.split(";")
        .list.eval(
            pl.element()
            .filter(pl.element().str.starts_with(f"{key}="))
            .first()  # Prend la première occurrence s'il y en avait plusieurs
        )
        .str.replace(f"^{key}=", "")
        .alias(key)
    )


# Chargement complet en Lazy
lf = pl.scan_csv(
    "Datasets/VCF_annovar.vcf",
    skip_rows=444,
    separator="\t",
    schema_overrides={"#CHROM": pl.Utf8},
)

# Extraction des clés dynamiques
for key in info_keys:
    lf = lf.with_columns(
        pl.col("INFO").str.extract(rf"{key}=([^;]*)").alias(key)
    )

# Optionnel : essai de cast (float, int, etc.)
for key in info_keys:
    lf = lf.with_columns(
        pl.col(key).cast(pl.Float64, strict=False).alias(key)  # Cast optionnel
    )


# -------------------
# Étape 3 - Ajout de colonnes spéciales (id incrémental et hash)
# -------------------

# Hash unique basé sur CHROM, POS, REF, ALT
lf = lf.with_columns(
    pl.concat_str(["#CHROM", "POS", "REF", "ALT"], separator="_")
    .map_elements(lambda x: hashlib.sha256(x.encode()).hexdigest(), return_dtype=pl.Utf8)
    .alias("hash")
)

# # Polars Lazy pur ne gère pas super bien les IDs auto-incrémentés en full streaming.
# # Solution partielle : arange en mode batch (si streaming total requis, une autre solution est nécessaire)
# lf = lf.with_columns(
#     pl.arange(0, lf.count()).alias("id")
# )

# -------------------
# Étape 4 - Export en Parquet
# -------------------

lf.sink_parquet("db/variants.parquet")

print("✅ Export parquet terminé")
