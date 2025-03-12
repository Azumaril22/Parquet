import json
import time

import requests
from django.http import HttpResponse
from django.views import View
from parquetapp.models import ParquetFile, ParquetFileColumn


def add_head_to_html(html):
    head = """<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="Brief summary of the page content">
    <meta name="keywords" content="objet, détails, page, content">
    <link
    rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css"
    >
    <style>
        :root {
            --pico-border-radius: 2rem;
            --pico-typography-spacing-vertical: 1.5rem;
            --pico-form-element-spacing-vertical: 1rem;
            --pico-form-element-spacing-horizontal: 1.25rem;
            --pico-font-size: 0.875rem;
        }
        pre{
            padding: 2rem;
            white-space: pre-wrap;
            word-wrap: break-word;
        }
        .Flag{
            color: green
        }
        .DOUBLE, .Float{
            color: yellow
        }
        .BIGINT, .Integer{
            color: orange
        }
    </style>
</head>
"""
    response_html = f"{head}<body>{html}</body></html>"
    return response_html


class PerformanceTestView(View):

    def get(self, request, *args, **kwargs):

        id_file = ParquetFile.objects.all().first().id

        base_url = f"http://127.0.0.1:8000/api/parquet-files/{id_file}"
        cases = [
            {
                "url": base_url + "/?page=1&page_size=1000&lier_fichiers=False",
                "nb_requests": 10,
                "test_case": "Durée de chargement 1000 enregistrements sur fichier non liés",
                "REQUEST_METHOD": "GET",
                "body": {},
            },
            {
                "url": base_url + "/?page=1&page_size=1000&lier_fichiers=True",
                "nb_requests": 10,
                "test_case": "Durée de chargement 1000 enregistrements sur fichier liés",
                "REQUEST_METHOD": "GET",
                "body": {},
            },
            {
                "url": base_url + "/?page=1&page_size=100&lier_fichiers=False",
                "nb_requests": 10,
                "test_case": "Durée de chargement 100 enregistrements sur fichier non liés",
                "REQUEST_METHOD": "GET",
                "body": {},
            },
            {
                "url": base_url + "/?page=1&page_size=100&lier_fichiers=True",
                "nb_requests": 10,
                "test_case": "Durée de chargement 100 enregistrements sur fichier liés",
                "REQUEST_METHOD": "GET",
                "body": {},
            },
            {
                "url": base_url + "/add-data/",
                "nb_requests": 1,
                "test_case": "Durée de traitement add 1 enregistrement",
                "REQUEST_METHOD": "POST",
                "body": {
                    "HASH": "00000000000000000000000000000",
                    "#CHROM": "christophe1",
                    "POS": 156594848,
                    "ID": "rs3795732",
                    "REF": "G",
                    "ALT": "A",
                    "QUAL": 3414.93,
                    "FILTER": "PASS",
                    "INFO": "TEST",
                },
            },
            # {
            #     "url": base_url + "/update-data/",
            #     "nb_requests": 1,
            #     "test_case": "Durée de traitement update 1 enregistrement",
            #     "REQUEST_METHOD": "POST",
            #     "body": {[
            #         {
            #             "HASH": "00000000000000000000000000000",
            #             "#CHROM": "christophe2",
            #             "POS": 156594848,
            #             "ID": "rs3795732",
            #             "REF": "G",
            #             "ALT": "A",
            #             "QUAL": 3414.93,
            #             "FILTER": "PASS",
            #             "INFO": "TEST"
            #         },
            #     ]},
            # },
            {
                "url": base_url + "/delete-data/",
                "nb_requests": 1,
                "test_case": "Durée de traitement delete 1 enregistrement",
                "REQUEST_METHOD": "POST",
                "body": {
                    "hashes": "00000000000000000000000000000",
                },
            },
        ]

        headers = {
            "Content-Type": "application/json",
        }

        # Listes pour stocker les logs
        logs = []
        morelogs = []
        test_case = ""

        # Envoi des x requêtes et mesure du temps d'execution
        for case in cases:
            total_time = 0
            nb_requests = case["nb_requests"]
            if "test_case" in case:
                test_case = '"' + case["test_case"] + '"'
            for i in range(nb_requests):
                start_time = time.time()

                if "GET" in case["REQUEST_METHOD"]:
                    response = requests.get(case["url"])

                elif "POST" in case["REQUEST_METHOD"]:
                    response = requests.post(
                        case["url"], data=json.dumps(case["body"]), headers=headers
                    )

                elif "PUT" in case["REQUEST_METHOD"]:
                    response = requests.put(
                        case["url"], data=json.dumps(case["body"]), headers=headers
                    )

                elif "DELETE" in case["REQUEST_METHOD"]:
                    response = requests.delete(case["url"])

                elif "PATCH" in case["REQUEST_METHOD"]:
                    response = requests.patch(
                        case["url"], data=json.dumps(case["body"]), headers=headers
                    )

                duration = time.time() - start_time
                total_time += duration

                log_entry = f"Requête {test_case} {i + 1}/{nb_requests} - Statut: {response.status_code} - Temps: {duration:.4f} secondes"
                logs.append(log_entry)

                # vérifier les statuts (et potentiellement arrêter en cas d'échec)
                if response.status_code not in [200, 201]:
                    morelogs.append(
                        f"ERREUR: La requête {test_case} {i + 1} a échoué avec le statut {response.status_code}"
                    )
                    json_payload = json.dumps(case["body"], indent=4)
                    morelogs.append(f"BODY {test_case}: {json_payload}")
                    morelogs.append(f"Requête {test_case} {i + 1}: {response.text}\n")
                    # break

            # Ajout du temps total
            logs.append(
                f"\nTemps total pour les {nb_requests} requêtes du cas {test_case} {cases.index(case) + 1} : {total_time:.4f} secondes soit {total_time / nb_requests:.4f} secondes en moyenne par requête\n"
            )

        # Retourner les logs comme une page HTML simple
        response_html = "<h1>Résultats du test de performance</h1><h2>Logs</h2><pre>{}</pre><h2>More logs</h2><pre>{}</pre>".format(
            "\n".join(logs), "\n".join(morelogs)
        )
        if "<!DOCTYPE html>" not in response_html:
            response_html = add_head_to_html(response_html)
        return HttpResponse(response_html, content_type="text/html; charset=utf-8")


class ListColumsInFile(View):
    def get(self, request, *args, **kwargs):
        columns = ParquetFileColumn.objects.all()
        response_html = "<table><thead><tr><th>File</th><th>Column Name</th><th>Type VCF</th><th>Type Parquet</th></tr></thead><tbody>"
        for column in columns:
            response_html += f"<tr><td>{column.parquet_file.file_path}</td><td>{column.name}</td><td class='{column.data_type_from_vcf}'>{column.data_type_from_vcf}</td><td class='{column.data_type_from_duckdb}'>{column.data_type_from_duckdb}</td></tr>"

        response_html += "</tbody></table>"

        response_html = add_head_to_html(response_html)
        return HttpResponse(response_html, content_type="text/html; charset=utf-8")