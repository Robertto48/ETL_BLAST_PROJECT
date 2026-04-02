import pandas as pd
import numpy as np
from datetime import timedelta


# =========================================================
# 0) Utilidades básicas
# =========================================================
def columnas_a_mayuscula_y_strip(df: pd.DataFrame) -> pd.DataFrame:
    """Pone columnas en mayúscula y elimina espacios laterales."""
    df = df.copy()
    df.columns = df.columns.astype(str).str.upper().str.strip()
    return df


def _encontrar_columna(df: pd.DataFrame, candidatos):
    """
    Devuelve el primer nombre de columna que exista en df, buscando en candidatos.
    candidatos: lista de strings (exact match) o lista de tuplas (varias opciones).
    """
    cols = set(df.columns)
    for c in candidatos:
        if isinstance(c, (list, tuple)):
            for cc in c:
                if cc in cols:
                    return cc
        else:
            if c in cols:
                return c
    return None


def _parsear_fecha_str(x) -> pd.Timestamp:
    """Convierte a Timestamp (fecha), si falla retorna NaT."""
    return pd.to_datetime(x, errors="coerce")


# =========================================================
# 1) Estandarización GTD (ground truth)
# =========================================================
def preparar_gtd(df_gtd: pd.DataFrame) -> pd.DataFrame:
    """
    - Pasa columnas a mayúscula/strip
    - Renombra columnas para tener: PLOT, NAME, BL1, BL2, BL3
    """
    df = columnas_a_mayuscula_y_strip(df_gtd)

    # Posibles nombres para PLOT en GTD
    col_plot = _encontrar_columna(df, [
        "PLOT",
        "ID",
        "OBS",
        "CONSECUTIVO",
        "EXP PLOT ORDER",
        "CREATE ID",
        "PLOT ENSAYO",
        "FILA (Y)",          # caso FLAR 2023
    ])

    # Posibles nombres para NAME (genotipo)
    col_name = _encontrar_columna(df, [
        "NAME",
        "MATERIAL",
        "DESIGNACION",
        "NOMBRE",
    ])

    # Posibles nombres para BLOCK (solo un caso especial)
    col_block = _encontrar_columna(df, [
        "BLOCK",
        "BLOQUE (X)",        # caso FLAR 2023
    ])

    df = df.copy()

    # Renombrar PLOT
    if col_plot is not None and col_plot != "PLOT":
        df.rename(columns={col_plot: "PLOT"}, inplace=True)

    # Renombrar NAME
    if col_name is not None and col_name != "NAME":
        df.rename(columns={col_name: "NAME"}, inplace=True)

    # Renombrar BLOCK si existe
    if col_block is not None and col_block != "BLOCK":
        df.rename(columns={col_block: "BLOCK"}, inplace=True)

    # Si NO hay PLOT, crearlo por secuencia (como en 2025)
    if "PLOT" not in df.columns:
        df["PLOT"] = range(1, len(df) + 1)

    # Dejar solo lo necesario (si faltan BL1..BL3, se crean como NaN)
    for bl in ["BL1", "BL2", "BL3"]:
        if bl not in df.columns:
            df[bl] = np.nan

    if "NAME" not in df.columns:
        # Si no hay NAME, se deja como NaN (no bloquea el pipeline)
        df["NAME"] = np.nan

    # Asegurar tipo numérico de PLOT si se puede
    df["PLOT"] = pd.to_numeric(df["PLOT"], errors="coerce")

    # Selección final de columnas (incluye BLOCK si existe)
    columnas_finales = ["PLOT", "NAME", "BL1", "BL2", "BL3"]
    if "BLOCK" in df.columns:
        columnas_finales.insert(1, "BLOCK")  # PLOT, BLOCK, NAME, BL1, BL2, BL3

    df = df[columnas_finales]
    return df


# =========================================================
# 2) Estandarización VIS (Pheno-i)
# =========================================================
def preparar_vis(df_vis: pd.DataFrame) -> pd.DataFrame:
    """
    - Pasa columnas a mayúscula/strip
    - Renombra ID -> PLOT si aplica
    - Convierte TIMESTAMP a fecha (YYYY-MM-DD string)
    - Mantiene todas las columnas de índices
    """
    df = columnas_a_mayuscula_y_strip(df_vis).copy()

    # PLOT suele venir como ID en VIS
    if "PLOT" not in df.columns and "ID" in df.columns:
        df.rename(columns={"ID": "PLOT"}, inplace=True)

    # Caso especial: VIS con PLOT y BLOCK (FLAR 2023)
    if "BLOCK" not in df.columns:
        # Algunos VIS tienen "BLOCK" explícito, otros no. Si no está, no hacemos nada.
        pass

    # TIMESTAMP debe existir
    if "TIMESTAMP" not in df.columns:
        raise ValueError("El archivo VIS no contiene la columna TIMESTAMP (después de normalizar).")

    # Convertir a fecha (string YYYY-MM-DD)
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce").dt.date.astype(str)

    return df


# =========================================================
# 3) Consolidación de fechas consecutivas
# =========================================================
def consolidar_fechas_consecutivas(df_vis: pd.DataFrame) -> pd.DataFrame:
    """
    Regla:
    - Si hay datos en dos días consecutivos (d y d+1), se mapean ambos a d (la fecha más antigua).
    - Solo se admite diferencia de 1 día para consolidar.
    """
    df = df_vis.copy()

    # Convertir TIMESTAMP a datetime para calcular diferencias
    fechas = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
    fechas_validas = sorted(set(fechas.dropna().dt.date))

    # Construir mapping: fecha_nueva -> fecha_base
    # Ej: 2025-06-21 -> 2025-06-20 si ambas existen y son consecutivas.
    mapping = {}

    i = 0
    while i < len(fechas_validas):
        base = fechas_validas[i]
        j = i
        # agrupar corrida consecutiva: base, base+1, base+2... (por seguridad)
        while (j + 1 < len(fechas_validas)) and ((fechas_validas[j + 1] - fechas_validas[j]).days == 1):
            j += 1

        # si hay corrida (más de 1 fecha), mapear todas al 'base'
        if j > i:
            for k in range(i, j + 1):
                mapping[str(fechas_validas[k])] = str(base)

        i = j + 1

    # Aplicar mapping (si una fecha no está en mapping, queda igual)
    if mapping:
        df["TIMESTAMP"] = df["TIMESTAMP"].replace(mapping)

    return df


# =========================================================
# 4) Asociar fechas a BL1/BL2/BL3 (modo auto y manual)
# =========================================================
def construir_mapeo_bl_por_fechas(
    fechas_vis_ordenadas,
    *,
    modo: str = "auto",
    bls_permitidos=(1, 2, 3),
    manual_bl_fecha: dict | None = None,
    min_gap_bl2: int = 6,
    big_gap_bl3: int = 13,
):
    """
    Retorna un dict: fecha_str -> "BL1"/"BL2"/"BL3" (o None si no se asigna)

    Reglas (modo auto):
    - BL1: fecha más temprana
    - BL2: primera fecha con diferencia >= min_gap_bl2 desde BL1
    - BL3: primera fecha con diferencia >= min_gap_bl2 desde BL2
    - Caso especial: si solo hay 2 fechas y la diferencia >= big_gap_bl3,
      entonces la 2da fecha se asocia a BL3 (BL2 no existe en dron)
    """
    fechas = [pd.to_datetime(f).date() for f in fechas_vis_ordenadas]
    fechas = sorted(set(fechas))
    mapeo = {}

    # Inicializar como no asignadas
    for f in fechas:
        mapeo[str(f)] = None

    if len(fechas) == 0:
        return mapeo

    # Modo manual: el usuario define BL -> fecha base
    if modo.lower() == "manual":
        if not manual_bl_fecha:
            raise ValueError("modo='manual' requiere manual_bl_fecha={bl:int -> fecha:'YYYY-MM-DD'}")

        for bl, fecha_str in manual_bl_fecha.items():
            if bl not in bls_permitidos:
                continue
            f = pd.to_datetime(fecha_str, errors="coerce")
            if pd.isna(f):
                continue
            f = f.date()
            if str(f) in mapeo:
                mapeo[str(f)] = f"BL{bl}"

        return mapeo

    # Modo auto
    # BL1 siempre es la más temprana
    f1 = fechas[0]
    if 1 in bls_permitidos:
        mapeo[str(f1)] = "BL1"

    # Caso especial: solo 2 fechas con salto grande -> BL3
    if len(fechas) == 2:
        f2 = fechas[1]
        gap = (f2 - f1).days
        if gap >= big_gap_bl3 and (3 in bls_permitidos):
            mapeo[str(f2)] = "BL3"
            return mapeo

    # Buscar BL2
    f_bl2 = None
    if 2 in bls_permitidos:
        for f in fechas[1:]:
            if (f - f1).days >= min_gap_bl2:
                f_bl2 = f
                mapeo[str(f)] = "BL2"
                break

    # Buscar BL3
    if 3 in bls_permitidos:
        # Si existe BL2, buscamos desde BL2; si no, buscamos desde BL1
        base = f_bl2 if f_bl2 is not None else f1
        for f in fechas:
            if f <= base:
                continue
            if (f - base).days >= min_gap_bl2:
                # Si ya era BL2, saltar
                if mapeo[str(f)] == "BL2":
                    continue
                mapeo[str(f)] = "BL3"
                break

    return mapeo


# =========================================================
# 5) Crear BL_INFECTION dentro del dataframe combinado
# =========================================================
def crear_bl_infection(df_merge: pd.DataFrame, mapeo_fecha_a_bl: dict) -> pd.DataFrame:
    """
    Crea BL_INFECTION a partir del mapping fecha->BLx.
    Conserva filas aunque BL quede en NaN.
    """
    df = df_merge.copy()

    # Importante: usar None para que la columna sea tipo "object"
    df["BL_INFECTION"] = None

    for fecha_str, bl_col in mapeo_fecha_a_bl.items():
        if bl_col is None:
            continue
        if bl_col not in df.columns:
            continue

        mask = df["TIMESTAMP"] == fecha_str

        # Forzar a vector 1D
        valores = df.loc[mask, bl_col].to_numpy().ravel()

        df.loc[mask, "BL_INFECTION"] = valores

    return df


def validar_bl_rango_1_9(df: pd.DataFrame, col_bl: str = "BL_INFECTION") -> pd.DataFrame:
    """
    BL válido: 1..9. Todo lo demás se convierte a NaN.
    Incluye casos como '-', 0, 65, texto, etc.
    """
    df = df.copy()
    df[col_bl] = pd.to_numeric(df[col_bl], errors="coerce")
    df.loc[(df[col_bl] < 1) | (df[col_bl] > 9), col_bl] = np.nan
    return df


# =========================================================
# 6) Transform principal por par GTD y VIS
# =========================================================
def transformar_par_gtd_vis(
    df_gtd_raw: pd.DataFrame,
    df_vis_raw: pd.DataFrame,
    *,
    location: str,
    year: int,
    original_dataset: str,
    modo_fechas: str = "auto",             # "auto" o "manual"
    bls_permitidos=(1, 2, 3),
    manual_bl_fecha: dict | None = None,   # ej. {2:"2023-06-13", 3:"2023-06-28"}
    min_gap_bl2: int = 6,
    big_gap_bl3: int = 13,
    return_metrics: bool = False
) -> pd.DataFrame:
    """
    Transforma y combina un par GTD y VIS para obtener el dataframe final por asociación.

    Salida:
    PLOT | BL_INFECTION | LOCATION | YEAR | ORIGINAL_DATASET | NAME | TIMESTAMP | (todos los índices VIS)
    """
    # 1) Preparación/normalización de cada fuente
    df_gtd = preparar_gtd(df_gtd_raw)
    df_vis = preparar_vis(df_vis_raw)

    # 2) Consolidar fechas consecutivas en VIS (d y d+1 -> d)
    df_vis = consolidar_fechas_consecutivas(df_vis)

    # 3) Definir llaves de merge
    # Caso especial: si ambos tienen BLOCK, usamos (PLOT, BLOCK)
    # (esto cubre FLAR 2023, y no afecta otros casos)
    merge_keys = ["PLOT"]
    if ("BLOCK" in df_gtd.columns) and ("BLOCK" in df_vis.columns):
        merge_keys = ["PLOT", "BLOCK"]

    # 4) Merge GTD + VIS
    df_merge = pd.merge(df_gtd, df_vis, on=merge_keys, how="inner")
    
    #KR1
    metrics = {
        "YEAR": year,
        "LOCATION": location,
        "ORIGINAL_DATASET": original_dataset,
        "MERGE_KEYS": "+".join(merge_keys)
    }
    metrics.update(calcular_kr1_cobertura(df_gtd, df_merge, merge_keys))

    # 5) Construir mapeo fecha->BLx
    fechas_vis = sorted(set(df_merge["TIMESTAMP"].dropna().tolist()))
    mapeo = construir_mapeo_bl_por_fechas(
        fechas_vis,
        modo=modo_fechas,
        bls_permitidos=bls_permitidos,
        manual_bl_fecha=manual_bl_fecha,
        min_gap_bl2=min_gap_bl2,
        big_gap_bl3=big_gap_bl3,
    )

    # 6) Crear BL_INFECTION
    df_merge = crear_bl_infection(df_merge, mapeo)

    # 7) Validar rango BL 1..9 (todo lo demás a NaN)
    df_merge = validar_bl_rango_1_9(df_merge, col_bl="BL_INFECTION")

    # 8) Eliminar columnas BL1..BL3 (ya no se necesitan)
    df_merge.drop(columns=["BL1", "BL2", "BL3"], errors="ignore", inplace=True)

    # 9) Si se usó BLOCK para merge, lo removemos del resultado final
    df_merge.drop(columns=["BLOCK"], errors="ignore", inplace=True)

    # 10) Agregar metadatos
    df_merge["LOCATION"] = location
    df_merge["YEAR"] = year
    df_merge["ORIGINAL_DATASET"] = original_dataset

    # 11) Ordenar columnas según estructura esperada
    # (todos los índices VIS quedan después de TIMESTAMP)
    base_cols = ["PLOT", "BL_INFECTION", "LOCATION", "YEAR", "ORIGINAL_DATASET", "NAME", "TIMESTAMP"]

    # Indices: todo lo demás (excepto base)
    indices_cols = [c for c in df_merge.columns if c not in base_cols]

    # Reordenar
    columnas_salida = base_cols + indices_cols
    df_out = df_merge[columnas_salida].copy()

    if return_metrics:
        return df_out, metrics
    return df_out

# ---------------------------------------------------------
# OKR / KPI - KR1: Cobertura de integración GTD y VIS
# ---------------------------------------------------------
def calcular_kr1_cobertura(df_gtd_preparado: pd.DataFrame,
                           df_merge: pd.DataFrame,
                           merge_keys: list) -> dict:
    """
    KR1: Integrar >= 95% de registros GTD con VIS usando llaves normalizadas.

    """

    # Total de llaves únicas en GTD
    total_llaves_gtd = df_gtd_preparado[merge_keys].drop_duplicates().shape[0]

    # Llaves únicas integradas
    llaves_integradas = df_merge[merge_keys].drop_duplicates().shape[0]

    if total_llaves_gtd == 0:
        cobertura_pct = 0.0
    else:
        cobertura_pct = (llaves_integradas / total_llaves_gtd) * 100.0

    return {
        "gtd_llaves_total": int(total_llaves_gtd),
        "gtd_llaves_integradas": int(llaves_integradas),
        "kr1_cobertura_pct": float(round(cobertura_pct, 4))
    }
    
# ---------------------------------------------------------
# OKR / KPI - KR2: Errores de esquema - columnas inconsistentes
# ---------------------------------------------------------
def evaluar_esquema_gtd(df_gtd_raw: pd.DataFrame) -> dict:
    """
    Evalúa si el archivo GTD cumple un esquema mínimo.
    Retorna dict con schema_ok y detalles de faltantes.
    """
    df = columnas_a_mayuscula_y_strip(df_gtd_raw)
    cols = set(df.columns)

    candidatos_plot = {"PLOT", "ID", "OBS", "CONSECUTIVO", "EXP PLOT ORDER", "CREATE ID", "PLOT ENSAYO", "FILA (Y)"}
    candidatos_name = {"NAME", "MATERIAL", "DESIGNACION", "NOMBRE"}
    candidatos_bl = {"BL1", "BL2", "BL3"}

    tiene_plot = len(cols & candidatos_plot) > 0
    tiene_name = len(cols & candidatos_name) > 0
    bl_presentes = list(cols & candidatos_bl)

    schema_ok = True
    problemas = []

    if not tiene_plot:
        schema_ok = False
        problemas.append("Falta columna (o alias) para PLOT")

    if not tiene_name:
        schema_ok = False
        problemas.append("Falta columna (o alias) para NAME")

    if len(bl_presentes) == 0:
        schema_ok = False
        problemas.append("Faltan BL1/BL2/BL3 (ninguna presente)")

    return {
        "tipo": "GTD",
        "schema_ok": schema_ok,
        "bl_presentes": ",".join(sorted(bl_presentes)) if bl_presentes else "",
        "problemas": "; ".join(problemas) if problemas else ""
    }


def evaluar_esquema_vis(df_vis_raw: pd.DataFrame) -> dict:
    """
    Evalúa si el archivo VIS cumple un esquema mínimo.
    Retorna dict con schema_ok y detalles de faltantes.
    """
    df = columnas_a_mayuscula_y_strip(df_vis_raw)
    cols = set(df.columns)

    tiene_timestamp = "TIMESTAMP" in cols
    tiene_plot = ("PLOT" in cols) or ("ID" in cols)
    tiene_ndvi_mean = "NDVI_MEAN" in cols

    schema_ok = True
    problemas = []

    if not tiene_timestamp:
        schema_ok = False
        problemas.append("Falta TIMESTAMP")

    if not tiene_plot:
        schema_ok = False
        problemas.append("Falta PLOT o ID")

    if not tiene_ndvi_mean:
        schema_ok = False
        problemas.append("Falta NDVI_MEAN (índice mínimo)")

    return {
        "tipo": "VIS",
        "schema_ok": schema_ok,
        "problemas": "; ".join(problemas) if problemas else ""
    }
