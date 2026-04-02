import os
import re
import pandas as pd

import extract
import transform
import load

import time
from datetime import datetime

# ---------------------------------------------------------
# Funciones para metadatos
# ---------------------------------------------------------
def extraer_year_de_carpeta(nombre_carpeta: str) -> int:
    """
    Espera nombres tipo: data_2024
    Retorna 2024 como int. Si falla, retorna -1.
    """
    m = re.search(r"data_(\d{4})", nombre_carpeta)
    if m:
        return int(m.group(1))
    return -1


def inferir_location_por_nombre(nombre_archivo: str) -> str:
    """
    Si el nombre contiene CIAT -> CIAT
    Si contiene FLAR -> FLAR
    Si no, UNKNOWN
    """
    name = nombre_archivo.upper()
    if "CIAT" in name:
        return "CIAT"
    if "FLAR" in name:
        return "FLAR"
    return "UNKNOWN"


def construir_original_dataset_desde_gtd(path_gtd: str) -> str:
    """
    ORIGINAL_DATASET = nombre del archivo GTD sin la cadena 'GTD' y sin extensión. Limpia separadores sobrantes.

    Ej:
    - GTD_Field_11_Block3_CIAT_2022.xlsx -> Field_11_Block3_CIAT_2022
    """
    base = os.path.splitext(os.path.basename(path_gtd))[0]  # sin extensión

    # 1) Quitar 'GTD' en cualquier parte 
    base = re.sub(r"(?i)gtd", "", base)

    # 2) Limpiar separadores repetidos (__, --, espacios múltiples)
    base = re.sub(r"[_\-]+", "_", base)     # convierte múltiples _ o - en un solo _
    base = re.sub(r"\s+", " ", base)        # colapsa espacios múltiples
    base = base.strip(" _-")                # quita separadores al inicio/fin

    return base


# ---------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------
def run_pipeline(root_path: str, output_path: str):
    # -----------------------------
    # KPIs: acumuladores de ejecución
    # -----------------------------
    kpi_rows = []
    run_start = time.perf_counter()
    errores_en_corrida = 0  # para KPI 3, éxito por ejecución

    # Buscar carpetas data_YYYY
    carpetas_year = [
        os.path.join(root_path, d)
        for d in os.listdir(root_path)
        if os.path.isdir(os.path.join(root_path, d)) and d.startswith("data_")
    ]

    audit_total = []

    # Ruta DB
    db_path = os.path.join(output_path, "db", "blast_etl.db")

    for path_anio in sorted(carpetas_year):
        nombre_carpeta = os.path.basename(path_anio)
        year = extraer_year_de_carpeta(nombre_carpeta)

        # 1) Emparejar archivos (EXTRACT)
        pares, audit_df = extract.emparejar_gtd_vis(path_anio, min_score=0.30)
        audit_total.append(audit_df)

        # Guardar auditoría por año
        out_audit_year = os.path.join(output_path, "audit", f"audit_{nombre_carpeta}.csv")
        load.guardar_auditoria(audit_df, out_audit_year)

        # 2) Procesar cada par
        for p in pares:
            path_gtd = p["gtd_path"]
            path_vis = p["vis_path"]

            # Metadatos (para reporte)
            location = inferir_location_por_nombre(os.path.basename(path_gtd))
            original_dataset = construir_original_dataset_desde_gtd(path_gtd)

            print("\n--- PROCESANDO ASOCIACION ---")
            print("Año:", year)
            print("Location:", location)
            print("GTD:", os.path.basename(path_gtd))
            print("VIS:", os.path.basename(path_vis))
            print("Original dataset:", original_dataset)
            print("Score match:", round(p["score"], 3))

            # -----------------------------
            # KPI 4: frescura (mtime de archivos fuente)
            # -----------------------------
            gtd_mtime = os.path.getmtime(path_gtd)
            vis_mtime = os.path.getmtime(path_vis)
            source_mtime = max(gtd_mtime, vis_mtime)
            source_mtime_iso = datetime.fromtimestamp(source_mtime).isoformat(timespec="seconds")

            # Flags por asociación (opción B)
            success_pair = 1
            error_msg = ""

            # -----------------------------
            # EXTRACT (lectura)
            # -----------------------------
            try:
                df_gtd = extract.leer_archivo_gtd(path_gtd)
                df_vis = extract.leer_archivo_vis(path_vis)
            except Exception as e:
                success_pair = 0
                error_msg = f"EXTRACT_ERROR: {e}"
                errores_en_corrida += 1

                # Registrar KPI (sin tiempos de transform/load)
                kpi_rows.append({
                    "YEAR": year,
                    "LOCATION": location,
                    "ORIGINAL_DATASET": original_dataset,
                    "gtd_file": os.path.basename(path_gtd),
                    "vis_file": os.path.basename(path_vis),
                    "success_pair": success_pair,
                    "error_msg": error_msg,
                    "transform_time_sec": None,
                    "csv_write_sec": None,
                    "sqlite_insert_sec": None,
                    "source_mtime": source_mtime_iso,
                    "load_timestamp": None,
                    "freshness_hours": None
                })
                print("[ERROR]", error_msg)
                continue  # seguir con el siguiente par

            # -----------------------------
            # KPI 1: tiempo de transformación
            # -----------------------------
            t0 = time.perf_counter()
            try:
                df_final, okr_metrics = transform.transformar_par_gtd_vis(
                    df_gtd_raw=df_gtd,
                    df_vis_raw=df_vis,
                    location=location,
                    year=year,
                    original_dataset=original_dataset,
                    modo_fechas="auto",
                    bls_permitidos=(1, 2, 3),
                    manual_bl_fecha=None,
                    min_gap_bl2=6,
                    big_gap_bl3=13,
                    return_metrics=True
                )
            except Exception as e:
                success_pair = 0
                error_msg = f"TRANSFORM_ERROR: {e}"
                errores_en_corrida += 1

                transform_sec = time.perf_counter() - t0

                kpi_rows.append({
                    "YEAR": year,
                    "LOCATION": location,
                    "ORIGINAL_DATASET": original_dataset,
                    "gtd_file": os.path.basename(path_gtd),
                    "vis_file": os.path.basename(path_vis),
                    "success_pair": success_pair,
                    "error_msg": error_msg,
                    "transform_time_sec": round(transform_sec, 4),
                    "csv_write_sec": None,
                    "sqlite_insert_sec": None,
                    "source_mtime": source_mtime_iso,
                    "load_timestamp": None,
                    "freshness_hours": None
                })
                print("[ERROR]", error_msg)
                continue

            transform_sec = time.perf_counter() - t0

            # -----------------------------
            # KPI 2: tiempo de carga CSV + SQLite
            # -----------------------------
            # Guardar CSV por asociación
            out_name = f"{nombre_carpeta}__{original_dataset}.csv"
            out_file = os.path.join(output_path, "processed", out_name)

            t1 = time.perf_counter()
            try:
                load.guardar_dataframe_csv(df_final, out_file)
                csv_sec = time.perf_counter() - t1
            except Exception as e:
                success_pair = 0
                error_msg = f"CSV_WRITE_ERROR: {e}"
                errores_en_corrida += 1
                csv_sec = time.perf_counter() - t1

                kpi_rows.append({
                    "YEAR": year,
                    "LOCATION": location,
                    "ORIGINAL_DATASET": original_dataset,
                    "gtd_file": os.path.basename(path_gtd),
                    "vis_file": os.path.basename(path_vis),
                    "success_pair": success_pair,
                    "error_msg": error_msg,
                    "transform_time_sec": round(transform_sec, 4),
                    "csv_write_sec": round(csv_sec, 4),
                    "sqlite_insert_sec": None,
                    "source_mtime": source_mtime_iso,
                    "load_timestamp": None,
                    "freshness_hours": None
                })
                print("[ERROR]", error_msg)
                continue

            # Insertar a SQLite
            load_timestamp = datetime.now().isoformat(timespec="seconds")

            t2 = time.perf_counter()
            try:
                load.guardar_en_sqlite(df_final, db_path, table_name="blast_observations")
                sqlite_sec = time.perf_counter() - t2
            except Exception as e:
                success_pair = 0
                error_msg = f"SQLITE_INSERT_ERROR: {e}"
                errores_en_corrida += 1
                sqlite_sec = time.perf_counter() - t2

                # Frescura se calcula igualmente porque “intentó cargar”
                freshness_hours = (datetime.fromisoformat(load_timestamp) - datetime.fromtimestamp(source_mtime)).total_seconds() / 3600.0

                kpi_rows.append({
                    "YEAR": year,
                    "LOCATION": location,
                    "ORIGINAL_DATASET": original_dataset,
                    "gtd_file": os.path.basename(path_gtd),
                    "vis_file": os.path.basename(path_vis),
                    "success_pair": success_pair,
                    "error_msg": error_msg,
                    "transform_time_sec": round(transform_sec, 4),
                    "csv_write_sec": round(csv_sec, 4),
                    "sqlite_insert_sec": round(sqlite_sec, 4),
                    "source_mtime": source_mtime_iso,
                    "load_timestamp": load_timestamp,
                    "freshness_hours": round(freshness_hours, 4)
                })
                print("[ERROR]", error_msg)
                continue

            # KPI 4: frescura (horas)
            freshness_hours = (datetime.fromisoformat(load_timestamp) - datetime.fromtimestamp(source_mtime)).total_seconds() / 3600.0

            # Registrar KPIs exitosos
            kpi_rows.append({
                "YEAR": year,
                "LOCATION": location,
                "ORIGINAL_DATASET": original_dataset,
                "gtd_file": os.path.basename(path_gtd),
                "vis_file": os.path.basename(path_vis),
                "success_pair": 1,
                "error_msg": "",
                "transform_time_sec": round(transform_sec, 4),
                "csv_write_sec": round(csv_sec, 4),
                "sqlite_insert_sec": round(sqlite_sec, 4),
                "source_mtime": source_mtime_iso,
                "load_timestamp": load_timestamp,
                "freshness_hours": round(freshness_hours, 4)
            })

            print("Salida guardada:", out_file)
            print("Shape final:", df_final.shape)

    # -----------------------------
    # Crear índices SQLite (una sola vez)
    # -----------------------------
    t_idx = time.perf_counter()
    try:
        load.crear_indice_sqlite(db_path, table_name="blast_observations")
        sqlite_index_sec = time.perf_counter() - t_idx
    except Exception as e:
        sqlite_index_sec = time.perf_counter() - t_idx
        errores_en_corrida += 1
        print("[WARN] Error creando índices SQLite:", e)

    # -----------------------------
    # Auditoría global
    # -----------------------------
    audit_all = pd.concat(audit_total, ignore_index=True)
    out_audit_all = os.path.join(output_path, "audit", "audit_total.csv")
    load.guardar_auditoria(audit_all, out_audit_all)

    # -----------------------------
    # Guardar KPIs
    # -----------------------------
    kpi_df = pd.DataFrame(kpi_rows)
    out_kpi = os.path.join(output_path, "metrics", "pipeline_kpis.csv")
    os.makedirs(os.path.dirname(out_kpi), exist_ok=True)
    kpi_df.to_csv(out_kpi, index=False)

    # -----------------------------
    # KPI 2: totales de carga
    # KPI 3: tasa de éxito
    # -----------------------------
    run_total_sec = time.perf_counter() - run_start

    total_pairs = len(kpi_df)
    ok_pairs = int(kpi_df["success_pair"].sum()) if total_pairs > 0 else 0
    success_rate_pairs = (ok_pairs / total_pairs) * 100 if total_pairs > 0 else 0.0

    # Éxito por ejecución: 1 si no hubo errores, 0 si hubo al menos 1
    success_run = 1 if errores_en_corrida == 0 else 0

    total_transform = float(kpi_df["transform_time_sec"].fillna(0).sum()) if "transform_time_sec" in kpi_df else 0.0
    total_csv = float(kpi_df["csv_write_sec"].fillna(0).sum()) if "csv_write_sec" in kpi_df else 0.0
    total_sqlite = float(kpi_df["sqlite_insert_sec"].fillna(0).sum()) if "sqlite_insert_sec" in kpi_df else 0.0

    print("\n=== KPIs Pipeline (Resumen) ===")
    print("Pares procesados:", total_pairs)
    print("Pares exitosos:", ok_pairs)
    print("Tasa éxito por pares (%):", round(success_rate_pairs, 3))
    print("Éxito ejecución (0/1):", success_run)
    print("Tiempo total ejecución (sec):", round(run_total_sec, 3))
    print("Tiempo total Transform (sec):", round(total_transform, 3))
    print("Tiempo total CSV (sec):", round(total_csv, 3))
    print("Tiempo total SQLite insert (sec):", round(total_sqlite, 3))
    print("Tiempo crear índices SQLite (sec):", round(sqlite_index_sec, 3))
    print("KPIs guardados en:", out_kpi)
    print("Auditoría total:", out_audit_all)
    
root_path = r"C:\Users\JCastano\OneDrive - Universidad Autonoma de Occidente\Archivos de ROBERTO JOSE GUERRERO CRIOLLO - ETL - PROYECTO COMPARTIDO\CODIGO\data\raw"
out_path = r"C:\Users\JCastano\OneDrive - Universidad Autonoma de Occidente\Archivos de ROBERTO JOSE GUERRERO CRIOLLO - ETL - PROYECTO COMPARTIDO\CODIGO\2\data\processed"

run_pipeline(root_path, out_path)

