#!/usr/bin/env python3
"""
Pipeline BCRA - Descarga y normaliza el archivo series.xlsm del Banco Central.

Produce un unico archivo en /data:
  bcra_hechos.csv   -> tabla de hechos en formato ANCHO (wide)
                       una fila por fecha, una columna por variable de nivel
                       Las variaciones se calculan con medidas DAX en Power BI.

Modelo Power BI:
  Tabla de hechos : bcra_hechos.csv  (fecha + 41 columnas numericas)
  Dimension unica : Calendario generado con CALENDARAUTO() en Power BI

Rezagos por hoja (verificados al 2026-04-19):
  TASAS DE MERCADO      -> diario,   ~3 dias rezago
  INSTRUMENTOS DEL BCRA -> diario,   ~3 dias rezago
  RESERVAS (saldos)     -> diario,   ~5 dias rezago
  BASE MONETARIA saldos -> diario,  ~18 dias rezago
  DEPOSITOS / PRESTAMOS -> mensual, ~49 dias rezago (fin de mes)
  Tasa politica monet.  -> discontinuada como serie separada desde ~jul 2025
"""

import csv
import io
import json
import os
import ssl
import sys
import urllib.request
from datetime import datetime, timezone

import openpyxl
from openpyxl.utils import column_index_from_string

BCRA_URL = (
    "https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/series.xlsm"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bcra.gob.ar/datos-monetarios-diarios/",
    "Accept": "application/vnd.ms-excel,*/*",
}

DATA_ROW_START = 10

# (hoja_excel, columna_excel, nombre_columna_csv, descripcion, unidad)
# Solo variables de NIVEL (saldos, tasas, tipo de cambio).
# Las variaciones porcentuales se calculan en DAX.
# [D] = diario  |  [M] = mensual (fin de mes)
COLUMNAS = [
    # RESERVAS [D] - millones USD
    ("RESERVAS", "C",  "res_total",             "Reservas internacionales - saldo total",        "millones USD"),
    ("RESERVAS", "D",  "res_oro_divisas",        "Reservas - oro, divisas y colocaciones",        "millones USD"),
    ("RESERVAS", "E",  "res_pase_pasivo",        "Reservas - pase pasivo USD exterior",           "millones USD"),
    ("RESERVAS", "N",  "res_degs",               "DEGs 2009 - saldo asignaciones",                "millones USD"),
    ("RESERVAS", "P",  "tc_bcra",                "Tipo de cambio BCRA (pesos por USD)",           "pesos/USD"),
    # BASE MONETARIA [D] - millones $
    ("BASE MONETARIA", "Z",  "bm_billetes_publico",   "BM - saldo billetes y monedas publico",   "millones $"),
    ("BASE MONETARIA", "AA", "bm_billetes_entidades", "BM - saldo billetes y monedas entidades", "millones $"),
    ("BASE MONETARIA", "AC", "bm_cta_cte_bcra",       "BM - saldo cuentas corrientes en BCRA",  "millones $"),
    ("BASE MONETARIA", "AD", "bm_saldo",               "BM - saldo base monetaria",              "millones $"),
    ("BASE MONETARIA", "AF", "bm_mas_cuasimonedas",    "BM - saldo base mas cuasimonedas",       "millones $"),
    ("DEPOSITOS",      "AC", "m2",                     "M2 - agregado monetario amplio",          "millones $"),
    ("DEPOSITOS",      "AD", "m2_transaccional_priv",  "M2 - transaccional privado",             "millones $"),
    # DEPOSITOS [M] - millones
    ("DEPOSITOS", "K",  "dep_cc_priv",            "Depositos - CC sector privado",               "millones $"),
    ("DEPOSITOS", "L",  "dep_ca_priv",            "Depositos - CA sector privado",               "millones $"),
    ("DEPOSITOS", "M",  "dep_pf_no_ajust_priv",   "Depositos - PF no ajustable privado",         "millones $"),
    ("DEPOSITOS", "N",  "dep_pf_ajust_cer_priv",  "Depositos - PF ajustable CER/UVA privado",    "millones $"),
    ("DEPOSITOS", "S",  "dep_total_pesos_priv",   "Depositos - total pesos sector privado",      "millones $"),
    ("DEPOSITOS", "U",  "dep_usd_priv_enpesos",   "Depositos - USD privado en pesos",            "millones $"),
    ("DEPOSITOS", "X",  "dep_total_priv_enpesos", "Depositos - total pesos y USD privado en $",  "millones $"),
    ("DEPOSITOS", "AA", "dep_usd_priv",           "Depositos - USD sector privado en USD",       "millones USD"),
    # PRESTAMOS [M] - millones
    ("PRESTAMOS", "B",  "prest_adelantos",        "Prestamos - adelantos CC pesos",              "millones $"),
    ("PRESTAMOS", "C",  "prest_documentos",       "Prestamos - documentos pesos",                "millones $"),
    ("PRESTAMOS", "D",  "prest_hipotecarios",     "Prestamos - hipotecarios pesos",              "millones $"),
    ("PRESTAMOS", "E",  "prest_prendarios",       "Prestamos - prendarios pesos",                "millones $"),
    ("PRESTAMOS", "F",  "prest_personales",       "Prestamos - personales pesos",                "millones $"),
    ("PRESTAMOS", "G",  "prest_tarjetas",         "Prestamos - tarjetas credito pesos",          "millones $"),
    ("PRESTAMOS", "H",  "prest_otros",            "Prestamos - otros pesos",                     "millones $"),
    ("PRESTAMOS", "I",  "prest_total_pesos",      "Prestamos - total pesos",                     "millones $"),
    ("PRESTAMOS", "Q",  "prest_total_usd",        "Prestamos - total USD en USD",                "millones USD"),
    ("PRESTAMOS", "U",  "prest_total_enpesos",    "Prestamos - total pesos y USD en pesos",      "millones $"),
    # TASAS DE MERCADO [D] - %
    ("TASAS DE MERCADO", "B",  "tasa_pf_total_tna",    "Tasa PF total general TNA",              "%"),
    ("TASAS DE MERCADO", "C",  "tasa_pf_personas_tna", "Tasa PF personas humanas TNA",           "%"),
    ("TASAS DE MERCADO", "L",  "badlar_total_tna",     "BADLAR total bancos TNA",                "%"),
    ("TASAS DE MERCADO", "M",  "badlar_privados_tna",  "BADLAR bancos privados TNA",             "%"),
    ("TASAS DE MERCADO", "R",  "tasa_personales_tna",  "Tasa prestamos personales TNA",          "%"),
    ("TASAS DE MERCADO", "S",  "tasa_adelantos_tna",   "Tasa adelantos empresas TNA",            "%"),
    # INSTRUMENTOS DEL BCRA [D] - mixto
    # Tasa politica monetaria discontinuada ~jul 2025
    ("INSTRUMENTOS DEL BCRA", "K",  "tasa_politica_tna", "Tasa politica monetaria TNA",         "%"),
    ("INSTRUMENTOS DEL BCRA", "L",  "tasa_politica_tea", "Tasa politica monetaria TEA",         "%"),
    ("INSTRUMENTOS DEL BCRA", "B",  "pases_pasivos",      "Pases pasivos - saldo total",        "millones $"),
    ("INSTRUMENTOS DEL BCRA", "F",  "leliq_notaliq",      "LELIQ y NOTALIQ - saldo",            "millones $"),
    ("INSTRUMENTOS DEL BCRA", "AV", "lefi_entidades",     "LEFI - cartera entidades",           "millones $"),
]


def descargar_xlsx(url):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=120, context=ctx) as resp:
        contenido = resp.read()
    if contenido[:2] != b"PK":
        raise ValueError(
            f"Respuesta inesperada del servidor (no es ZIP/Excel). "
            f"Primeros bytes: {contenido[:50]}"
        )
    return contenido


def extraer_serie(ws, col_letra, row_start=DATA_ROW_START):
    col_idx = column_index_from_string(col_letra.strip())
    datos = {}
    for row in ws.iter_rows(min_row=row_start, values_only=True):
        fecha_raw = row[0]
        valor_raw = row[col_idx - 1]
        if not isinstance(fecha_raw, datetime):
            continue
        if valor_raw is None or not isinstance(valor_raw, (int, float)):
            continue
        datos[fecha_raw.strftime("%Y-%m-%d")] = float(valor_raw)
    return datos


def main():
    print("=" * 60)
    print("Pipeline BCRA")
    print("=" * 60)

    print(f"\nDescargando: {BCRA_URL}")
    try:
        contenido = descargar_xlsx(BCRA_URL)
    except Exception as exc:
        print(f"\nERROR al descargar: {exc}", file=sys.stderr)
        sys.exit(1)
    print(f"Descarga OK: {len(contenido) / 1_000_000:.1f} MB")

    wb = openpyxl.load_workbook(io.BytesIO(contenido), read_only=True, keep_vba=False)
    print(f"Hojas: {wb.sheetnames}\n")

    series = {}
    todas_las_fechas = set()
    errores = []
    ultima_fecha_por_col = {}

    for (hoja, col, nombre_col, descripcion, unidad) in COLUMNAS:
        if hoja not in wb.sheetnames:
            errores.append(f"[CRITICO] Hoja '{hoja}' no encontrada (columna '{nombre_col}')")
            series[nombre_col] = {}
            continue
        ws = wb[hoja]
        try:
            datos = extraer_serie(ws, col)
        except Exception as exc:
            errores.append(f"[CRITICO] Error en '{nombre_col}' (hoja={hoja}, col={col}): {exc}")
            series[nombre_col] = {}
            continue
        if not datos:
            errores.append(f"[AVISO] '{nombre_col}': 0 filas (hoja='{hoja}', col='{col}')")
        series[nombre_col] = datos
        todas_las_fechas.update(datos.keys())
        ultima = max(datos.keys()) if datos else "N/A"
        ultima_fecha_por_col[nombre_col] = ultima
        print(f"  {nombre_col:<30} | {len(datos):6,d} filas | hasta {ultima}")

    wb.close()

    nombres_col = [c[2] for c in COLUMNAS]
    fechas_ordenadas = sorted(todas_las_fechas)
    print(f"\nFechas unicas: {len(fechas_ordenadas):,}  |  Columnas: {len(nombres_col)}")

    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)

    # Tabla de hechos en formato ancho
    with open(os.path.join(output_dir, "bcra_hechos.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha"] + nombres_col)
        for fecha in fechas_ordenadas:
            fila = [fecha] + [series[col].get(fecha, "") for col in nombres_col]
            writer.writerow(fila)

    # Metadata de columnas (referencia; no se importa al modelo Power BI)
    with open(os.path.join(output_dir, "bcra_columnas.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["columna", "descripcion", "unidad", "ultima_fecha"])
        for (hoja, col, nombre_col, descripcion, unidad) in COLUMNAS:
            writer.writerow([nombre_col, descripcion, unidad, ultima_fecha_por_col.get(nombre_col, "")])

    # JSON de control de actualizacion
    resumen = {
        "pipeline_ejecutado_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_fechas": len(fechas_ordenadas),
        "primera_fecha": fechas_ordenadas[0] if fechas_ordenadas else None,
        "ultima_fecha_global": fechas_ordenadas[-1] if fechas_ordenadas else None,
        "ultima_fecha_por_columna": ultima_fecha_por_col,
    }
    with open(os.path.join(output_dir, "bcra_last_update.json"), "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)

    print(f"\nbcra_hechos.csv  : {len(fechas_ordenadas):,} filas x {len(nombres_col) + 1} columnas")
    print(f"bcra_columnas.csv: {len(COLUMNAS)} columnas documentadas")
    print(f"bcra_last_update.json: generado")

    if errores:
        print("\nERRORES / AVISOS:")
        for e in errores:
            print(f"  {e}", file=sys.stderr)
        sys.exit(1)

    print("\nPipeline BCRA completado exitosamente.")


if __name__ == "__main__":
    main()
