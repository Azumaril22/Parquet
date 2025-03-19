import duckdb
from dev_tools.utils import timeit
from parquetapp.models import ParquetFile
from parquetapp.utils.data_type import get_column_dtype


def get_bdml_for_file(filename, file_path):
    bdml = ""
    bdml += f"Table {filename}.parquet "
    bdml += "{\r\n"

    for col in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").fetchall():
        dtype = get_column_dtype(
            datatable=file_path,
            col=col[0],
            dtype=col[1]
        )
        clean_col_name = col[0].replace('-', '_').replace('+', '_').replace(" ", "_")
        bdml += f"    {clean_col_name} {str(dtype).lower()}\r\n"

    bdml += "}\r\n"

    return bdml

@timeit
def create_bdml(vcf_file_path):
    bdml = ""

    for file in ParquetFile.objects.filter(original_vcf_file_path=vcf_file_path):
        if "compile" not in file.file_path:
            export_path = file.file_path
            bdml += get_bdml_for_file(
                file.name,
                file.file_path
            )
            bdml += "\r\n"

    bdml += "\r\n"

    if "variant" in bdml and "info_variant" in bdml:
        bdml += "Ref: variant.parquet.HASH < info_variant.parquet.HASH\r\n"

    if "variant" in bdml and "sample_variant" in bdml:
        bdml += "Ref: variant.parquet.HASH < sample_variant.parquet.HASH\r\n"

    if "info_variant" in bdml and "info_csq_variant" in bdml:
        bdml += "Ref: info_variant.parquet.HASH < info_csq_variant.parquet.HASH\r\n"

    if "variant" in bdml and "sample_variant_unpivot" in bdml:
        bdml += "Ref: variant.parquet.HASH - sample_variant_unpivot.parquet.HASH\r\n"

    if "sample_variant_unpivot" in bdml and "sample_variant" in bdml:
        bdml += "Ref: sample_variant.parquet.HASH > sample_variant_unpivot.parquet.HASH\r\n"

    bdml_path = "/".join(export_path.split('/')[:-1]) + "/bdd_model.dbml"
    with open(bdml_path, "w") as f:
        f.write(bdml)
