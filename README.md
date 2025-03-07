TESTS Basés sur les travaux de "Sacha Schutz" de ce billet https://dridk.me/parquet-files.html


Prérequis : 
    installer python3
    installer pip
    installer jupyter si vous souhaitez utiliser les fichiers de tests présent dans le dossier "Jupyter"
    
Créer un environnement virtuel avec python :
    exemple : python3 -m venv .venv

L'activer :
    exemple sur linux : source .venv/bin/activate

Installer les dépendances :
    pip install -r requirements.txt

Créer un dossier db/ à la racine du projet
Créer un dossier Datasets/ à la racine du projet et y placer les VCF à parser.

07/03/25
Dernier SCRIPT à jour et fonctionnel : "vcf2parquet/vcf2parquet_v2.py"