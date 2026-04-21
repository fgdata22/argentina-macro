#!/usr/bin/env python3
"""
datos_gob_comercio_pipeline.py
Descarga el Intercambio Comercial Argentino (ICA) desde la API de datos.gob.ar.

Fuente: INDEC - ICA (dataset 74), vía API datos.gob.ar.
  - Exportaciones e importaciones totales (USD millones)
  - Saldo comercial
  - Aperturas por grandes rubros: PP, MOA, MOI, C&E (expo);
    BK, BI, CyL, Piezas, BC, Vehículos, Resto (impo)

Salida:
  - data/datos_gob_comercio_hechos.csv
  - data/datos_gob_comercio_last_update.json
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
START_DATE = "2003-01"   # ICA cubre desde 2003
LIMIT      = 5000

SERIES = [
    # === Totales ===
    ("74.3_IET_0_M_16",    "ica_expo_total",    "Exportaciones totales",                            "Millones USD"),
    ("74.3_IIT_0_M_25",    "ica_impo_total",    "Importaciones totales",                            "Millones USD"),
    ("74.3_ISC_0_M_19",    "ica_saldo",         "Saldo comercial (expo - impo)",                    "Millones USD"),

    # === Exportaciones por rubro ===
    ("74.3_IEPP_0_M_35",   "ica_expo_pp",       "Exportaciones - Productos primarios (PP)",          "Millones USD"),
    ("74.3_IEMOA_0_M_48",  "ica_expo_moa",      "Exportaciones - Manuf. de origen agropecuario (MOA)", "Millones USD"),
    ("74.3_IEMOI_0_M_46",  "ica_expo_moi",      "Exportaciones - Manuf. de origen industrial (MOI)",   "Millones USD"),
    ("74.3_IECE_0_M_35",   "ica_expo_ce",       "Exportaciones - Combustibles y energia (C&E)",     "Millones USD"),

    # === Importaciones por uso económico ===
    ("74.3_IIBCA_0_M_32",  "ica_impo_bk",       "Importaciones - Bienes de capital (BK)",           "Millones USD"),
    ("74.3_IIBI_0_M_36",   "ica_impo_bi",       "Importaciones - Bienes intermedios (BI)",          "Millones USD"),
    ("74.3_IICL_0_M_42",   "ica_impo_cyl",      "Importaciones - Combustibles y lubricantes (CyL)", "Millones USD"),
    ("74.3_IIPABC_0_M_50", "ica_impo_piezas",   "Importaciones - Piezas y accesorios de BK",        "Millones USD"),
    ("74.3_IIBCO_0_M_32",  "ica_impo_bc",       "Importaciones - Bienes de consumo (BC)",           "Millones USD"),
    ("74.3_IIVAP_0_M_49",  "ica_impo_vehic",    "Importaciones - Vehiculos automotores pasajeros",  "Millones USD"),
    ("74.3_IIR_0_M_23",    "ica_impo_resto",    "Importaciones - Resto",                            "Millones USD"),
]

DATA_DIR  = Path(__file__).parent.parent / "data"
CSV_PATH  = DATA_DIR / "datos_gob_comercio_hechos.csv"
JSON_PATH = DATA_DIR / "datos_gob_comercio_last_update.json"


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

    log(f"Consultando API datos.gob.ar para {len(SERIES)} series de comercio exterior (ICA)...")

    datos_por_id: dict[str, dict[str, float]] = {}

    for i in range(0, len(SERIES), BATCH_SIZE):
        batch     = SERIES[i : i + BATCH_SIZE]
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
        "pipeline":             "datos_gob_comercio",
        "ultima_actualizacion": timestamp_utc(),
        "total_filas":          n,
        "total_columnas":       len(todas_columnas) - 1,
        "fecha_inicio":         fechas_ord[0]  if fechas_ord else None,
        "fecha_fin":            fechas_ord[-1] if fechas_ord else None,
        "fuente":               "API Series de Tiempo - datos.gob.ar (ICA/INDEC)",
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

    log("Pipeline comercio exterior (ICA) completado sin errores.")


if __name__ == "__main__":
    main()
