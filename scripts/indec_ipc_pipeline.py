#!/usr/bin/env python3
"""
indec_ipc_pipeline.py
Descarga y normaliza el IPC (Índice de Precios al Consumidor) del INDEC.

Fuente (FTP INDEC, URL estable):
  - serie_ipc_divisiones.csv  → serie histórica con todas las divisiones COICOP

Filtros aplicados:
  - Solo región "Nacional"
  - Solo columna Indice_IPC (base dic 2016 = 100); las variaciones se calculan con DAX

Salida:
  - data/indec_ipc_hechos.csv         wide format: fecha + 18 columnas numéricas
  - data/indec_ipc_last_update.json
"""

import csv
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import (
    descargar_archivo, escribir_csv, escribir_json,
    log, log_error, periodo_yyyymm_a_fecha, timestamp_utc,
)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

IPC_URL = "https://www.indec.gob.ar/ftp/cuadros/economia/serie_ipc_divisiones.csv"

REGION_FILTRO = "Nacional"
COLUMNA_VALOR = "Indice_IPC"

# Mapeo: código en el CSV → nombre de columna en el CSV de salida.
# El orden aquí define el orden de columnas en el CSV final.
CODIGOS_IPC = {
    "0":          "ipc_general",
    "01":         "ipc_alimentos",
    "02":         "ipc_bebidas_tabaco",
    "03":         "ipc_indumentaria",
    "04":         "ipc_vivienda",
    "05":         "ipc_equipamiento",
    "06":         "ipc_salud",
    "07":         "ipc_transporte",
    "08":         "ipc_comunicacion",
    "09":         "ipc_recreacion",
    "10":         "ipc_educacion",
    "11":         "ipc_restaurantes",
    "12":         "ipc_otros",
    "B":          "ipc_bienes",
    "S":          "ipc_servicios",
    "Núcleo":     "ipc_nucleo",
    "Estacional": "ipc_estacional",
    "Regulados":  "ipc_regulados",
}

DATA_DIR  = Path(__file__).parent.parent / "data"
CSV_PATH  = DATA_DIR / "indec_ipc_hechos.csv"
JSON_PATH = DATA_DIR / "indec_ipc_last_update.json"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    errores: list = []

    # 1. Descargar
    log("Descargando IPC divisiones...")
    try:
        contenido = descargar_archivo(IPC_URL)
        log(f"  OK — {len(contenido):,} bytes")
    except RuntimeError as e:
        log_error(str(e))
        sys.exit(1)

    # 2. Parsear CSV
    # El archivo usa encoding latin-1 y separador punto y coma.
    log("Parseando CSV...")
    texto  = contenido.decode("latin-1")
    reader = csv.DictReader(io.StringIO(texto), delimiter=";")

    # Pivot acumulativo: {fecha: {nombre_col: valor}}
    datos: dict = {}
    filas_leidas = 0
    filas_usadas = 0

    for row in reader:
        filas_leidas += 1

        if row.get("Region", "").strip() != REGION_FILTRO:
            continue

        codigo = row.get("Codigo", "").strip()
        if codigo not in CODIGOS_IPC:
            continue

        periodo = row.get("Periodo", "").strip()
        fecha   = periodo_yyyymm_a_fecha(periodo)
        if fecha is None:
            errores.append(f"Período no reconocido: '{periodo}'")
            continue

        valor_raw = row.get(COLUMNA_VALOR, "").strip()
        try:
            # INDEC usa coma como separador decimal ("101,5859") → convertir a punto
            valor = float(valor_raw.replace(",", ".")) if valor_raw not in ("", "NA") else None
        except ValueError:
            valor = None

        if fecha not in datos:
            datos[fecha] = {}
        datos[fecha][CODIGOS_IPC[codigo]] = valor
        filas_usadas += 1

    log(f"  {filas_leidas:,} filas leidas -> {filas_usadas:,} usadas -> {len(datos)} fechas")

    # 3. Construir wide format
    nombres_cols   = list(CODIGOS_IPC.values())
    todas_columnas = ["fecha"] + nombres_cols
    fechas_ord     = sorted(datos.keys())

    filas = []
    for fecha in fechas_ord:
        fila = {"fecha": fecha}
        fila.update(datos[fecha])
        filas.append(fila)

    # 4. Escribir CSV
    n = escribir_csv(CSV_PATH, filas, todas_columnas)
    log(f"CSV escrito: {CSV_PATH.name} ({n} filas × {len(todas_columnas)} columnas)")

    # Validar cobertura
    for col in nombres_cols:
        con_dato = sum(1 for f in filas if f.get(col) is not None)
        if con_dato == 0:
            errores.append(f"Columna '{col}' sin ningún dato")
        else:
            log(f"  {col}: {con_dato} valores")

    # 5. Metadata
    metadata = {
        "pipeline":             "indec_ipc",
        "ultima_actualizacion": timestamp_utc(),
        "total_filas":          n,
        "total_columnas":       len(todas_columnas) - 1,
        "fecha_inicio":         fechas_ord[0]  if fechas_ord else None,
        "fecha_fin":            fechas_ord[-1] if fechas_ord else None,
        "fuente":               IPC_URL,
        "region":               REGION_FILTRO,
        "errores":              errores,
    }
    escribir_json(JSON_PATH, metadata)
    log(f"JSON escrito: {JSON_PATH.name}")

    if errores:
        log_error(f"{len(errores)} error(es):")
        for e in errores:
            log_error(f"  {e}")
        sys.exit(1)

    log("Pipeline IPC completado sin errores.")


if __name__ == "__main__":
    main()
