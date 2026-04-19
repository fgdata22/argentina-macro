#!/usr/bin/env python3
"""
indec_emae_pipeline.py
Descarga y normaliza el EMAE (Estimador Mensual de Actividad Económica) del INDEC.

Fuentes (FTP INDEC, URLs estables):
  - sh_emae_mensual_base2004.xls   → índice agregado (original + desest + tendencia)
  - sh_emae_actividad_base2004.xls → desglose sectorial (sectores A–O + impuestos)

Salida:
  - data/indec_emae_hechos.csv         wide format: fecha + 19 columnas numéricas
  - data/indec_emae_last_update.json
"""

import sys
from pathlib import Path

import xlrd

sys.path.insert(0, str(Path(__file__).parent))
from utils import (
    descargar_archivo, escribir_csv, escribir_json,
    log, log_error, mes_nombre_a_numero, timestamp_utc,
)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

EMAE_URL_MENSUAL   = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls"
EMAE_URL_ACTIVIDAD = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_actividad_base2004.xls"

HOJA_MENSUAL   = "Tabla"
HOJA_ACTIVIDAD = "Tabla Letras"

# Fila a partir de la cual empiezan los datos (0-indexed).
# Filas 0-4 son: título, vacía, encabezados, vacía, vacía.
FILA_DATOS = 5

# (índice de columna 0-based, nombre en el CSV, descripción legible)
COLS_MENSUAL = [
    (2, "emae_original",  "EMAE - índice serie original (2004=100)"),
    (4, "emae_desest",    "EMAE - índice serie desestacionalizada (2004=100)"),
    (6, "emae_tendencia", "EMAE - índice tendencia-ciclo (2004=100)"),
]

COLS_ACTIVIDAD = [
    (2,  "emae_agro",        "EMAE - A: Agricultura, ganadería, caza y silvicultura"),
    (3,  "emae_pesca",       "EMAE - B: Pesca"),
    (4,  "emae_mineria",     "EMAE - C: Explotación de minas y canteras"),
    (5,  "emae_industria",   "EMAE - D: Industria manufacturera"),
    (6,  "emae_elect",       "EMAE - E: Electricidad, gas y agua"),
    (7,  "emae_construccion","EMAE - F: Construcción"),
    (8,  "emae_comercio",    "EMAE - G: Comercio mayorista, minorista y reparaciones"),
    (9,  "emae_hoteles",     "EMAE - H: Hoteles y restaurantes"),
    (10, "emae_transporte",  "EMAE - I: Transporte y comunicaciones"),
    (11, "emae_finanzas",    "EMAE - J: Intermediación financiera"),
    (12, "emae_inmuebles",   "EMAE - K: Actividades inmobiliarias, empresariales y de alquiler"),
    (13, "emae_adm_publica", "EMAE - L: Administración pública y defensa"),
    (14, "emae_educacion",   "EMAE - M: Enseñanza"),
    (15, "emae_salud",       "EMAE - N: Servicios sociales y de salud"),
    (16, "emae_otros_svc",   "EMAE - O: Otras actividades de servicios comunitarios"),
    (17, "emae_impuestos",   "EMAE - Impuestos netos de subsidios"),
]

DATA_DIR  = Path(__file__).parent.parent / "data"
CSV_PATH  = DATA_DIR / "indec_emae_hechos.csv"
JSON_PATH = DATA_DIR / "indec_emae_last_update.json"

# ---------------------------------------------------------------------------
# Extracción
# ---------------------------------------------------------------------------

def extraer_series_xls(contenido: bytes, nombre_hoja: str, columnas: list) -> dict:
    """
    Parsea un XLS desde bytes y extrae las columnas indicadas.

    La fecha se construye haciendo forward-fill del año (col 0, float como 2004.0)
    más el nombre del mes en español (col 1).

    Devuelve: {fecha_iso: {nombre_col: float_o_None, ...}}
    """
    wb = xlrd.open_workbook(file_contents=contenido)
    ws = wb.sheet_by_name(nombre_hoja)

    datos: dict = {}
    anio_actual = None

    for r in range(FILA_DATOS, ws.nrows):
        val_anio = ws.cell_value(r, 0)
        val_mes  = ws.cell_value(r, 1)

        # Forward-fill del año: solo se escribe en la primera fila del año
        if isinstance(val_anio, float) and val_anio > 1900:
            anio_actual = int(val_anio)

        if anio_actual is None:
            continue

        # Convertir nombre del mes a número (descarta notas al pie y filas vacías)
        if not isinstance(val_mes, str):
            continue
        nro_mes = mes_nombre_a_numero(val_mes)
        if nro_mes is None:
            continue

        fecha = f"{anio_actual:04d}-{nro_mes:02d}-01"

        fila: dict = {}
        for (col_idx, nombre, _) in columnas:
            try:
                v = ws.cell_value(r, col_idx)
                fila[nombre] = float(v) if isinstance(v, (int, float)) else None
            except IndexError:
                fila[nombre] = None

        datos[fecha] = fila

    return datos


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    errores: list = []

    # 1. Descargar
    log("Descargando EMAE mensual (índice agregado)...")
    try:
        bytes_mensual = descargar_archivo(EMAE_URL_MENSUAL)
        log(f"  OK — {len(bytes_mensual):,} bytes")
    except RuntimeError as e:
        log_error(str(e))
        sys.exit(1)

    log("Descargando EMAE actividad sectorial...")
    try:
        bytes_actividad = descargar_archivo(EMAE_URL_ACTIVIDAD)
        log(f"  OK — {len(bytes_actividad):,} bytes")
    except RuntimeError as e:
        log_error(str(e))
        sys.exit(1)

    # 2. Extraer
    log("Extrayendo serie mensual agregada...")
    datos_mensual = extraer_series_xls(bytes_mensual, HOJA_MENSUAL, COLS_MENSUAL)
    log(f"  {len(datos_mensual)} fechas")

    log("Extrayendo series sectoriales...")
    datos_actividad = extraer_series_xls(bytes_actividad, HOJA_ACTIVIDAD, COLS_ACTIVIDAD)
    log(f"  {len(datos_actividad)} fechas")

    # 3. Combinar (outer join por fecha)
    todas_fechas = sorted(set(datos_mensual) | set(datos_actividad))
    log(f"Combinando: {len(todas_fechas)} fechas en total")

    nombres_mensual   = [c[1] for c in COLS_MENSUAL]
    nombres_actividad = [c[1] for c in COLS_ACTIVIDAD]
    todas_columnas    = ["fecha"] + nombres_mensual + nombres_actividad

    filas = []
    for fecha in todas_fechas:
        fila = {"fecha": fecha}
        fila.update(datos_mensual.get(fecha, {}))
        fila.update(datos_actividad.get(fecha, {}))
        filas.append(fila)

    # 4. Escribir CSV
    n = escribir_csv(CSV_PATH, filas, todas_columnas)
    log(f"CSV escrito: {CSV_PATH.name} ({n} filas × {len(todas_columnas)} columnas)")

    # Validar que ninguna columna quedó completamente vacía
    for col in nombres_mensual + nombres_actividad:
        con_dato = sum(1 for f in filas if f.get(col) is not None)
        if con_dato == 0:
            errores.append(f"Columna '{col}' sin ningún dato")
        else:
            log(f"  {col}: {con_dato} valores")

    # 5. Escribir JSON de metadata
    metadata = {
        "pipeline":             "indec_emae",
        "ultima_actualizacion": timestamp_utc(),
        "total_filas":          n,
        "total_columnas":       len(todas_columnas) - 1,
        "fecha_inicio":         todas_fechas[0]  if todas_fechas else None,
        "fecha_fin":            todas_fechas[-1] if todas_fechas else None,
        "fuentes": {
            "mensual":   EMAE_URL_MENSUAL,
            "actividad": EMAE_URL_ACTIVIDAD,
        },
        "errores": errores,
    }
    escribir_json(JSON_PATH, metadata)
    log(f"JSON escrito: {JSON_PATH.name}")

    if errores:
        log_error(f"{len(errores)} error(es):")
        for e in errores:
            log_error(f"  {e}")
        sys.exit(1)

    log("Pipeline EMAE completado sin errores.")


if __name__ == "__main__":
    main()
