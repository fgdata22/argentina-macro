#!/usr/bin/env python3
"""
datos_gob_precios_pipeline.py
Descarga índices de precios mayoristas (IPIM e IPIB) desde la API de datos.gob.ar.

Fuente: Sistema de Índices de Precios Mayoristas (SIPM) - INDEC vía datos.gob.ar.
  - IPIM: Índice de Precios Internos al por Mayor (base Dic-2015=100)
  - IPIB: Índice de Precios Internos Básicos del Productor (base Dic-2015=100)

Salida:
  - data/datos_gob_precios_hechos.csv
  - data/datos_gob_precios_last_update.json
"""

import csv
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import (
    descargar_archivo, escribir_csv, escribir_json,
    log, log_error, timestamp_utc,
)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

API_BASE   = "https://apis.datos.gob.ar/series/api/series/"
BATCH_SIZE = 20
START_DATE = "2016-01"
LIMIT      = 5000

SERIES = [
    # === IPIM ===
    ("448.1_NIVEL_GENERAL_0_0_13_46", "ipim_nivel_general",
     "IPIM - Nivel general (base Dic-2015=100)", "Índice"),

    # === IPIB ===
    ("450.1_NIVEL_GENERAL_0_0_13_92", "ipib_nivel_general",
     "IPIB - Nivel general (base Dic-2015=100)", "Índice"),
]

DATA_DIR  = Path(__file__).parent.parent / "data"
CSV_PATH  = DATA_DIR / "datos_gob_precios_hechos.csv"
JSON_PATH = DATA_DIR / "datos_gob_precios_last_update.json"


# ---------------------------------------------------------------------------
# Consulta a la API
# ---------------------------------------------------------------------------

def consultar_batch(ids: list[str]) -> dict[str, dict[str, float]]:
    ids_str = ",".join(ids)
    url = f"{API_BASE}?ids={ids_str}&format=csv&start_date={START_DATE}&limit={LIMIT}"
    contenido = descargar_archivo(url)
    texto = contenido.decode("utf-8")

    reader = csv.reader(io.StringIO(texto))
    filas  = list(reader)
    if not filas:
        return {}

    resultado: dict[str, dict[str, float]] = {}
    for fila in filas[1:]:
        if len(fila) < 1 + len(ids):
            continue
        fecha = fila[0].strip()
        if not fecha:
            continue
        valores_dict = {}
        for i, sid in enumerate(ids):
            raw = fila[i + 1].strip()
            try:
                valores_dict[sid] = float(raw) if raw else None
            except ValueError:
                valores_dict[sid] = None
        resultado[fecha] = valores_dict

    return resultado


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    errores: list = []

    log(f"Consultando API datos.gob.ar para {len(SERIES)} series de precios mayoristas...")

    datos_por_id: dict[str, dict[str, float]] = {}

    for i in range(0, len(SERIES), BATCH_SIZE):
        batch   = SERIES[i : i + BATCH_SIZE]
        ids_batch = [s[0] for s in batch]
        log(f"  Lote {i//BATCH_SIZE + 1}: {len(ids_batch)} series")
        try:
            resp = consultar_batch(ids_batch)
        except RuntimeError as e:
            log_error(f"Error consultando lote: {e}")
            errores.append(f"Fallo lote {i//BATCH_SIZE + 1}: {e}")
            continue
        for fecha, valores in resp.items():
            if fecha not in datos_por_id:
                datos_por_id[fecha] = {}
            datos_por_id[fecha].update(valores)

    log(f"  Total fechas obtenidas: {len(datos_por_id)}")

    id_a_col       = {s[0]: s[1] for s in SERIES}
    nombres_cols   = [s[1] for s in SERIES]
    todas_columnas = ["fecha"] + nombres_cols
    fechas_ord     = sorted(datos_por_id.keys())

    filas = []
    for fecha in fechas_ord:
        fila = {"fecha": fecha}
        for sid, valor in datos_por_id[fecha].items():
            col = id_a_col.get(sid)
            if col:
                fila[col] = valor
        filas.append(fila)

    n = escribir_csv(CSV_PATH, filas, todas_columnas)
    log(f"CSV escrito: {CSV_PATH.name} ({n} filas x {len(todas_columnas)} columnas)")

    for col in nombres_cols:
        con_dato = sum(1 for f in filas if f.get(col) is not None)
        if con_dato == 0:
            errores.append(f"Columna '{col}' sin ningun dato")
        else:
            log(f"  {col}: {con_dato} valores")

    metadata = {
        "pipeline":             "datos_gob_precios",
        "ultima_actualizacion": timestamp_utc(),
        "total_filas":          n,
        "total_columnas":       len(todas_columnas) - 1,
        "fecha_inicio":         fechas_ord[0]  if fechas_ord else None,
        "fecha_fin":            fechas_ord[-1] if fechas_ord else None,
        "fuente":               "API Series de Tiempo - datos.gob.ar (SIPM/INDEC)",
        "series_config":        [
            {"id": s[0], "columna": s[1], "descripcion": s[2], "unidad": s[3]}
            for s in SERIES
        ],
        "errores": errores,
    }
    escribir_json(JSON_PATH, metadata)
    log(f"JSON escrito: {JSON_PATH.name}")

    if errores:
        log_error(f"{len(errores)} error(es):")
        for e in errores:
            log_error(f"  {e}")
        sys.exit(1)

    log("Pipeline precios mayoristas completado sin errores.")


if __name__ == "__main__":
    main()
