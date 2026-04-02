import os
import pandas as pd
import sqlite3

def guardar_auditoria(audit_df: pd.DataFrame, out_path: str):
    """
    Guarda auditoría en CSV.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    audit_df.to_csv(out_path, index=False)


def guardar_dataframe_csv(df: pd.DataFrame, out_path: str):
    """
    Guarda dataframe en CSV.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)


def guardar_dataframe_excel(df: pd.DataFrame, out_path: str, sheet_name: str = "data"):
    """
    Guarda dataframe en Excel (una hoja).
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)


def guardar_en_sqlite(df: pd.DataFrame, db_path: str, table_name: str = "blast_observations"):
    """
    Inserta df en SQLite.
    Si la tabla existe, inserta SOLO columnas que ya existen en la tabla
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)

    try:
        df = df.copy()

        # Quitar columnas que a veces molestan
        df.drop(columns=["ID"], errors="ignore", inplace=True)

        cur = conn.cursor()

        # ¿Existe la tabla?
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        existe = cur.fetchone() is not None

        if not existe:
            # Crear tabla con el primer dataframe
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            return

        # Leer columnas existentes en la tabla
        cur.execute(f"PRAGMA table_info({table_name});")
        cols_tabla = [row[1] for row in cur.fetchall()]  # nombre columna

        # Alinear: quedarnos solo con columnas existentes
        cols_df = df.columns.tolist()
        comunes = [c for c in cols_df if c in cols_tabla]
        nuevas = [c for c in cols_df if c not in cols_tabla]

        if len(nuevas) > 0:
            print("[WARN] Columnas NO insertadas (no existen en la tabla):", nuevas)

        df_insert = df[comunes].copy()

        # Insertar
        df_insert.to_sql(table_name, conn, if_exists="append", index=False)

    finally:
        conn.close()


def crear_indice_sqlite(db_path: str, table_name: str = "blast_observations"):
    
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        # Índices recomendados para tus filtros típicos:
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name}(YEAR);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_dataset ON {table_name}(ORIGINAL_DATASET);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(TIMESTAMP);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_plot ON {table_name}(PLOT);")

        conn.commit()
    finally:
        conn.close()

def leer_tabla_sqlite(db_path: str, table_name: str = "blast_observations") -> pd.DataFrame:
    """
    Lee una tabla completa desde SQLite y retorna un DataFrame.
    """
    conn = sqlite3.connect(db_path)

    try:
        # Validar existencia de tabla
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
            (table_name,)
        )
        existe = cur.fetchone() is not None
        if not existe:
            raise ValueError(f"La tabla '{table_name}' no existe en la base de datos.")

        # Leer tabla
        df = pd.read_sql(f"SELECT * FROM {table_name};", conn)
        return df

    finally:
        conn.close()
        
import numpy as np
import pandas as pd


def reporte_sanidad_df(df: pd.DataFrame, top_n: int = 10) -> dict:
    """
    Genera un reporte de sanidad (data quality) a partir de un DataFrame.

    Incluye:
    - filas/columnas
    - nulos por columna (conteo y %)
    - top N columnas con más nulos
    - nulos/vacíos en columnas clave
    - BL fuera de rango (1..9) si existe BL_INFECTION
    """
    reporte = {}

    # -------------------------
    # 1) Info general
    # -------------------------
    n_rows = df.shape[0]
    n_cols = df.shape[1]
    reporte["info_general"] = {
        "filas": n_rows,
        "columnas": n_cols,
        "lista_columnas": df.columns.tolist()
    }

    # -------------------------
    # 2) Conteo de nulos por columna
    # -------------------------
    # Nulos estándar (NaN/None)
    null_count = df.isna().sum()

    # Vacíos en columnas tipo texto: "", "   "
    empty_count = pd.Series(0, index=df.columns)
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        empty_count[c] = df[c].astype(str).str.strip().eq("").sum()

    # Total “faltantes” = nulos + vacíos
    missing_count = null_count + empty_count

    # Porcentaje
    if n_rows > 0:
        missing_pct = (missing_count / n_rows) * 100
    else:
        missing_pct = missing_count * 0

    df_missing = pd.DataFrame({
        "columna": df.columns,
        "n_nulos": null_count.values,
        "n_vacios_texto": empty_count.values,
        "n_faltantes_total": missing_count.values,
        "pct_faltantes_total": missing_pct.values
    }).sort_values("n_faltantes_total", ascending=False).reset_index(drop=True)

    reporte["faltantes_por_columna"] = df_missing
    reporte["top_columnas_faltantes"] = df_missing.head(top_n)

    # -------------------------
    # 3) Revisión de columnas clave
    # -------------------------
    columnas_clave = ["PLOT", "BL_INFECTION", "LOCATION", "YEAR", "ORIGINAL_DATASET", "NAME", "TIMESTAMP"]
    cols_presentes = [c for c in columnas_clave if c in df.columns]

    df_clave = df_missing[df_missing["columna"].isin(cols_presentes)].copy()
    reporte["faltantes_columnas_clave"] = df_clave.reset_index(drop=True)

    # -------------------------
    # 4) BL fuera de rango (1..9)
    # -------------------------
    bl_fuera_rango = None
    if "BL_INFECTION" in df.columns:
        bl_num = pd.to_numeric(df["BL_INFECTION"], errors="coerce")
        mask_fuera = bl_num.notna() & ((bl_num < 1) | (bl_num > 9))
        bl_fuera_rango = int(mask_fuera.sum())
    reporte["bl_fuera_rango_1_9"] = bl_fuera_rango

    return reporte

def imprimir_reporte_sanidad(reporte: dict):
    """
    Imprime reporte de sanidad.
    """
    info = reporte["info_general"]
    print("\n=== REPORTE DE SANIDAD ===")
    print("Filas:", info["filas"])
    print("Columnas:", info["columnas"])

    print("\n--- Top columnas con más faltantes (nulos + vacíos) ---")
    print(reporte["top_columnas_faltantes"][["columna", "n_faltantes_total", "pct_faltantes_total"]])

    print("\n--- Faltantes en columnas clave ---")
    print(reporte["faltantes_columnas_clave"][["columna", "n_faltantes_total", "pct_faltantes_total"]])

    if reporte["bl_fuera_rango_1_9"] is not None:
        print("\n--- BL fuera de rango (1..9) ---")
        print("n_fuera_rango:", reporte["bl_fuera_rango_1_9"])