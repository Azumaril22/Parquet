import re


class VCFEnteteToPython:
    def __init__(self, filepath):
        self.filepath = filepath

    def parse_vcf_info_headers(self):
        info_dicts = []

        # Expression régulière pour extraire le contenu entre les chevrons
        info_pattern = re.compile(r'##INFO=<(.+?)>')

        # Expressions régulières pour extraire les attributs spécifiques
        id_pattern = re.compile(r'ID=([^,]+)')
        number_pattern = re.compile(r'Number=([^,]+)')
        type_pattern = re.compile(r'Type=([^,]+)')
        description_pattern = re.compile(r'Description=([^,]+)')

        with open(self.filepath, 'r') as vcf_file:
            for line in vcf_file:
                # Ne traiter que les lignes qui commencent par ##INFO=
                if line.startswith('##INFO='):
                    # Extraire le contenu entre les chevrons
                    match = info_pattern.search(line)
                    if match:
                        content = match.group(1)

                        # Initialiser un dictionnaire pour stocker les attributs
                        info_dict = {}

                        # Extraire ID
                        id_match = id_pattern.search(content)
                        if id_match:
                            info_dict["ID"] = id_match.group(1).replace('.', '_')

                        # Extraire Number
                        number_match = number_pattern.search(content)
                        if number_match:
                            info_dict["Number"] = number_match.group(1)

                        # Extraire Type
                        type_match = type_pattern.search(content)
                        if type_match:
                            info_dict["Type"] = type_match.group(1)

                        # Extraire Description
                        description_match = description_pattern.search(content)
                        if description_match:
                            info_dict["Description"] = description_match.group(1).replace('"', '')

                        # Ajouter le dictionnaire à la liste
                        info_dicts.append(info_dict)

                # Arrêter la lecture une fois que les en-têtes sont terminés
                elif not line.startswith('#'):
                    break

        return info_dicts
