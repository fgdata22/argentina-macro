#!/usr/bin/env python3
"""
datos_gob_pipeline.py
Descarga series de tiempo desde la API de datos.gob.ar (Ministerio de Economía).

Ventaja arquitectónica: un único script que consume una API REST homogénea,
con un listado de IDs de series como única configuración. Agregar una serie
nueva al dashboard es agregar una fila a la lista SERIES — no requiere tocar
lógica de parsing ni descargar archivos.

Salida:
  - data/datos_gob_fiscal_hechos.csv         wide format: fecha + 21 columnas
  - data/datos_gob_fiscal_last_update.json
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

API_BASE = "https://apis.datos.gob.ar/series/api/series/"

# La API impone un límite de caracteres por URL y ~40 series por request.
# Batcheamos por las dudas aunque hoy tengamos 21.
BATCH_SIZE = 20

# Series del IMIG (Informe Mensual de Ingresos y Gastos) del Min. Economía.
# El orden aquí define el orden de columnas en el CSV final.
# (id_serie, nombre_columna, descripción_legible, unidad)
SERIES = [
    # === Resultados ===
    ("452.3_RESULTADO_RIO_0_M_18_54",  "fiscal_res_primario",      "Resultado primario del SPN",                      "millones $"),
    ("452.3_RESULTADO_PIP_0_M_22_71",  "fiscal_res_primario_pip",  "Resultado primario con P.I.P. (Meta FMI)",        "millones $"),
    ("452.3_RESULTADO_ERO_0_M_20_25",  "fiscal_res_financiero",    "Resultado financiero del SPN",                    "millones $"),
    ("452.3_INTERESES_TOS_0_M_15_62",  "fiscal_intereses_netos",   "Intereses netos de la deuda pública",             "millones $"),

    # === Ingresos tributarios ===
    ("452.2_GANANCIASIAS_0_T_9_51",    "fiscal_ing_ganancias",     "Impuesto a las Ganancias",                        "millones $"),
    ("452.2_IVA_NETO_RROS_0_T_19_67",  "fiscal_ing_iva_neto",      "IVA neto de reintegros",                          "millones $"),
    ("452.2_APORTES_COIAL_0_T_39_29",  "fiscal_ing_aportes_ss",    "Aportes y contribuciones a la seguridad social",  "millones $"),
    ("452.2_DERECHOS_EION_0_T_20_42",  "fiscal_ing_retenciones",   "Derechos de exportación (retenciones)",           "millones $"),
    ("452.2_DERECHOS_IION_0_T_20_60",  "fiscal_ing_der_import",    "Derechos de importación",                         "millones $"),
    ("452.2_DEBITOS_CRTOS_0_T_16_22",  "fiscal_ing_deb_cred",      "Débitos y créditos (impuesto al cheque)",         "millones $"),
    ("452.2_BIENES_PERLES_0_T_17_26",  "fiscal_ing_bp",            "Bienes personales",                               "millones $"),

    # === Otros ingresos ===
    ("452.2_INGRESOS_NIOS_0_T_23_2",   "fiscal_ing_no_trib",       "Ingresos no tributarios",                         "millones $"),
    ("452.2_INGRESOS_CTAL_0_T_16_75",  "fiscal_ing_capital",       "Ingresos de capital",                             "millones $"),

    # === Prestaciones sociales ===
    ("452.2_JUBILACIONVAS_0_T_36_18",  "fiscal_gto_jubilaciones",  "Jubilaciones y pensiones contributivas",          "millones $"),
    ("452.2_ASIGNACIONIJO_0_T_26_67",  "fiscal_gto_asig_fam",      "Asignaciones familiares y AUH",                   "millones $"),
    ("452.2_PENSIONES_VAS_0_T_26_164", "fiscal_gto_pens_nc",       "Pensiones no contributivas",                      "millones $"),
    ("452.2_PRESTACIONSJP_0_T_19_86",  "fiscal_gto_pami",          "Prestaciones INSSJP (PAMI)",                      "millones $"),

    # === Subsidios económicos ===
    ("452.2_ENERGIAGIA_0_T_7_56",      "fiscal_gto_subs_energia",  "Subsidios económicos - energía",                  "millones $"),
    ("452.2_TRANSPORTERTE_0_T_10_32",  "fiscal_gto_subs_transp",   "Subsidios económicos - transporte",               "millones $"),

    # === Gastos de funcionamiento ===
    ("452.2_SALARIOSIOS_0_T_8_22",     "fiscal_gto_salarios",      "Salarios del Sector Público Nacional",            "millones $"),
    ("452.2_OTROS_GASTNTO_0_T_27_55",  "fiscal_gto_otros_func",    "Otros gastos de funcionamiento",                  "millones $"),
]

START_DATE = "2016-01"   # rango más temprano que garantizan las series IMIG
LIMIT      = 5000        # suficiente para ~400 años de datos mensuales

DATA_DIR  = Path(__file__).parent.parent / "data"
CSV_PATH  = DATA_DIR / "datos_gob_fiscal_hechos.csv"
JSON_PATH = DATA_DIR / "datos_gob_fiscal_last_update.json"


# ---------------------------------------------------------------------------
# Consulta a la API
# ---------------------------------------------------------------------------

def consultar_batch(ids: list[str]) -> dict[str, dict[str, float]]:
    """
    Consulta un lote de IDs de series a la API y devuelve un dict:
        {fecha_iso: {id_serie: valor, ...}, ...}

    La API retorna CSV con formato:
        indice_tiempo,<col1>,<col2>,...
        2016-01-01,val1,val2,...

    Las columnas están en el mismo orden que los IDs solicitados, por eso
    mapeamos por POSICIÓN en lugar de por nombre de columna de la API.
    """
    ids_str = ",".join(ids)
    url = f"{API_BASE}?ids={ids_str}&format=csv&start_date={START_DATE}&limit={LIMIT}"
    contenido = descargar_archivo(url)
    texto = contenido.decode("utf-8")

    reader = csv.reader(io.StringIO(texto))
    filas = list(reader)
    if not filas:
        return {}

    header = filas[0]
    # header[0] = 'indice_tiempo'; header[1..] = nombres auto-generados por la API
    # Usamos los IDs originales como claves internas (más estable).

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

    # 1. Traer todas las series en lotes
    log(f"Consultando API datos.gob.ar para {len(SERIES)} series...")

    # dict unificado: {fecha: {id_serie: valor}}
    datos_por_id: dict[str, dict[str, float]] = {}

    for i in range(0, len(SERIES), BATCH_SIZE):
        batch = SERIES[i : i + BATCH_SIZE]
        ids_batch = [s[0] for s in batch]
        log(f"  Lote {i//BATCH_SIZE + 1}: {len(ids_batch)} series")
        try:
            resp = consultar_batch(ids_batch)
        except RuntimeError as e:
            log_error(f"Error consultando lote: {e}")
            errores.append(f"Fallo lote {i//BATCH_SIZE + 1}: {e}")
            continue
        # Mergear en datos_por_id
        for fecha, valores in resp.items():
            if fecha not in datos_por_id:
                datos_por_id[fecha] = {}
            datos_por_id[fecha].update(valores)

    log(f"  Total fechas obtenidas: {len(datos_por_id)}")

    # 2. Construir filas en wide format usando los nombres_columna definidos localmente
    id_a_col = {s[0]: s[1] for s in SERIES}
    nombres_cols   = [s[1] for s in SERIES]
    todas_columnas = ["fecha"] + nombres_cols
    fechas_ord     = sorted(datos_por_id.keys())

    filas = []
    for fecha in fechas_ord:
        fila = {"fecha": fecha}
        for sid, valor in datos_por_id[fecha].items():
            col = id_a_col.get(sid)
            if col is not None:
                fila[col] = valor
        filas.append(fila)

    # 3. Escribir CSV
    n = escribir_csv(CSV_PATH, filas, todas_columnas)
    log(f"CSV escrito: {CSV_PATH.name} ({n} filas x {len(todas_columnas)} columnas)")

    # Validar cobertura por columna
    for col in nombres_cols:
        con_dato = sum(1 for f in filas if f.get(col) is not None)
        if con_dato == 0:
            errores.append(f"Columna '{col}' sin ningun dato")
        else:
            log(f"  {col}: {con_dato} valores")

    # 4. Metadata
    metadata = {
        "pipeline":             "datos_gob_fiscal",
        "ultima_actualizacion": timestamp_utc(),
        "total_filas":          n,
        "total_columnas":       len(todas_columnas) - 1,
        "fecha_inicio":         fechas_ord[0]  if fechas_ord else None,
        "fecha_fin":            fechas_ord[-1] if fechas_ord else None,
        "fuente":               "API Series de Tiempo - datos.gob.ar",
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

    log("Pipeline datos.gob.ar fiscal completado sin errores.")


if __name__ == "__main__":
    main()
