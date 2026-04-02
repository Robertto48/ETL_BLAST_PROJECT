import os
import re
import pandas as pd


# ---------------------------------------------------------
# 1) Clasificación de archivos
# ---------------------------------------------------------
def clasificar_archivo(nombre_archivo: str) -> str:
    """
    Regla acordada:
    - VIS: si el nombre contiene 'vis' (VIS, VIs, vis)
    - GTD: si el nombre contiene 'gtd' (GTD, gtd)
    """
    name = nombre_archivo.lower()
    if "vis" in name:
        return "VIS"
    if "gtd" in name:
        return "GTD"
    return "UNKNOWN"


# ---------------------------------------------------------
# 2) Listar archivos por año
# ---------------------------------------------------------
def listar_archivos_por_year(path_year: str):
    """
    Retorna dos listas:
    - lista_gtd: archivos GTD (ground truth)
    - lista_vis: archivos VIS (índices de vegetación)
    """
    extensiones_validas = (".xlsx", ".xls", ".xlsm", ".xlsb", ".csv")

    lista_gtd = []
    lista_vis = []

    for f in os.listdir(path_year):
        full = os.path.join(path_year, f)
        if not os.path.isfile(full):
            continue
        if not f.lower().endswith(extensiones_validas):
            continue

        tipo = clasificar_archivo(f)
        if tipo == "GTD":
            lista_gtd.append(full)
        elif tipo == "VIS":
            lista_vis.append(full)

    return sorted(lista_gtd), sorted(lista_vis)


# ---------------------------------------------------------
# 3) Normalizar nombres para emparejar por similitud
# ---------------------------------------------------------
def normalizar_nombre_para_match(nombre_sin_extension: str) -> str:
    """
    Normaliza el nombre para poder emparejar GTD con VIS por similitud
    """
    s = nombre_sin_extension.lower()
    s = s.replace("_", " ").replace("-", " ")

    # quitar caracteres no alfanuméricos
    s = re.sub(r"[^a-z0-9\s]+", " ", s)

    # quitar palabras ruidosas
    tokens = [t for t in s.split() if t not in ("gtd", "vis", "blast")]
    s = " ".join(tokens)

    s = re.sub(r"\s+", " ", s).strip()
    return s


def score_similitud(nombre_a: str, nombre_b: str) -> float:
    """
    Similaridad SIMPLE:
    - usa Jaccard entre conjuntos de palabras
    Retorna valor entre 0 y 1.
    """
    a = set(normalizar_nombre_para_match(nombre_a).split())
    b = set(normalizar_nombre_para_match(nombre_b).split())
    if len(a | b) == 0:
        return 0.0
    return len(a & b) / len(a | b)


# ---------------------------------------------------------
# 4) Emparejar archivos GTD y VIS dentro de un año
# ---------------------------------------------------------
def emparejar_gtd_vis(path_year: str, min_score: float = 0.30):
    """
    Empareja cada GTD con el VIS más similar.
    Retorna:
    - pares: lista de dicts con gtd_path, vis_path, score, year_folder
    - audit_df: dataframe con pares y no emparejados
    """
    lista_gtd, lista_vis = listar_archivos_por_year(path_year)

    usados_vis = set()
    pares = []

    audit_rows = []

    # Intentar emparejar cada GTD con el VIS más similar
    for gtd_path in lista_gtd:
        gtd_name = os.path.splitext(os.path.basename(gtd_path))[0]

        mejor_vis = None
        mejor_score = -1

        for vis_path in lista_vis:
            if vis_path in usados_vis:
                continue

            vis_name = os.path.splitext(os.path.basename(vis_path))[0]
            sc = score_similitud(gtd_name, vis_name)

            if sc > mejor_score:
                mejor_score = sc
                mejor_vis = vis_path

        if (mejor_vis is not None) and (mejor_score >= min_score):
            usados_vis.add(mejor_vis)
            pares.append({
                "year_folder": os.path.basename(path_year),
                "gtd_path": gtd_path,
                "vis_path": mejor_vis,
                "score": mejor_score
            })
            audit_rows.append({
                "year_folder": os.path.basename(path_year),
                "gtd_file": os.path.basename(gtd_path),
                "vis_file": os.path.basename(mejor_vis),
                "match_score": mejor_score,
                "status": "PAIRED"
            })
        else:
            audit_rows.append({
                "year_folder": os.path.basename(path_year),
                "gtd_file": os.path.basename(gtd_path),
                "vis_file": None,
                "match_score": mejor_score if mejor_score >= 0 else None,
                "status": "UNMATCHED_GTD"
            })

    # VIS que quedaron sin usar
    for vis_path in lista_vis:
        if vis_path not in usados_vis:
            audit_rows.append({
                "year_folder": os.path.basename(path_year),
                "gtd_file": None,
                "vis_file": os.path.basename(vis_path),
                "match_score": None,
                "status": "UNMATCHED_VIS"
            })

    audit_df = pd.DataFrame(audit_rows)
    return pares, audit_df


# ---------------------------------------------------------
# 5) Lectura de archivos (solo lectura, sin transformar)
# ---------------------------------------------------------
def leer_archivo_vis(path_vis: str) -> pd.DataFrame:
    """
    VIS:
    - Si es CSV: separado por coma
    - Si es Excel: primera hoja
    """
    if path_vis.lower().endswith(".csv"):
        return pd.read_csv(path_vis, sep=",")
    return pd.read_excel(path_vis, sheet_name=0)


def leer_archivo_gtd(path_gtd: str) -> pd.DataFrame:
    """
    GTD:
    - Excel primera hoja
    """
    if path_gtd.lower().endswith(".csv"):
        return pd.read_csv(path_gtd, sep=",")
    return pd.read_excel(path_gtd, sheet_name=0)