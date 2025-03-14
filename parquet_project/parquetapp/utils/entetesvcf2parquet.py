import re


class VCFEnteteToPython:
    def __init__(self, filepath):
        self.filepath = filepath

    def run(self):
        entetes_lignes_a_parser = ["INFO", "FILTER"]
        parsed_columns = [
            {
                "Entete": None,
                "ID": "HASH",
                "Number": None,
                "Type": "String",
                "Description": "HASH sha256 of concat CHROM_POS_REF_ALT",
            }
        ]
        for entete_ligne_a_parser in entetes_lignes_a_parser:
            parsed_columns += self.parse_vcf_headers(entete_ligne_a_parser)
        return parsed_columns

    def parse_vcf_headers(self, entete_ligne_a_parser="INFO"):
        parsed_dicts = []

        # Expression régulière pour extraire le contenu entre les chevrons
        info_pattern = re.compile(rf"##{entete_ligne_a_parser}=<(.+?)>")

        # Expressions régulières pour extraire les attributs spécifiques
        id_pattern = re.compile(r"ID=([^,]+)")
        number_pattern = re.compile(r"Number=([^,]+)")
        type_pattern = re.compile(r"Type=([^,]+)")
        description_pattern = re.compile(r"Description=([^,]+)")

        with open(self.filepath, "r") as vcf_file:
            for line in vcf_file:
                # Ne traiter que les lignes qui commencent par ##{entete_ligne_a_parser}=
                if line.startswith(f"##{entete_ligne_a_parser}="):
                    # Extraire le contenu entre les chevrons
                    match = info_pattern.search(line)
                    if match:
                        content = match.group(1)

                        # Initialiser un dictionnaire pour stocker les attributs
                        parsed_dict = {"Entete": entete_ligne_a_parser}

                        # Extraire ID
                        id_match = id_pattern.search(content)
                        if id_match:
                            parsed_dict["ID"] = id_match.group(1).replace(".", "_")

                        # Extraire Number
                        number_match = number_pattern.search(content)
                        if number_match:
                            parsed_dict["Number"] = number_match.group(1)

                        # Extraire Type
                        type_match = type_pattern.search(content)
                        if type_match:
                            parsed_dict["Type"] = type_match.group(1)

                        # Extraire Description
                        description_match = description_pattern.search(content)
                        if description_match:
                            parsed_dict["Description"] = description_match.group(
                                1
                            ).replace('"', "")

                        # Ajouter le dictionnaire à la liste
                        parsed_dicts.append(parsed_dict)

                # Arrêter la lecture une fois que les en-têtes sont terminés
                elif not line.startswith("#"):
                    break

        return parsed_dicts
