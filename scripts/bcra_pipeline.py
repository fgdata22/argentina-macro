#!/usr/bin/env python3
"""
Pipeline BCRA - Descarga y normaliza el archivo series.xlsm del Banco Central.

Produce dos archivos en /data:
  bcra_hechos.csv    -> tabla de hechos en formato largo (fecha, id_variable, valor)
  bcra_variables.csv -> dimension de variables (id, nombre, categoria, unidad)

Rezagos observados por hoja (al 2026-04-19):
  TASAS DE MERCADO     -> diario,   ~3 dias de rezago
  INSTRUMENTOS DEL BCRA-> diario,   ~3 dias de rezago
  RESERVAS (saldos)    -> diario,   ~5 dias de rezago
  BASE MONETARIA saldos-> diario,   ~18 dias de rezago
  BASE MONETARIA vars  -> diario,   ~108 dias de rezago (posiblemente descontinuado)
  DEPOSITOS / PRESTAMOS-> mensual,  ~49 dias de rezago (dato de fin de mes)
  Tasa politica monet. -> discontinuada como serie separada desde ~jul 2025

El pipeline falla (exit code 1) si no puede extraer datos de alguna variable,
lo que dispara una notificacion en GitHub Actions para intervencion manual.
"""

import csv
import io
import json
import os
import ssl
import sys
import urllib.request
from datetime import datetime, date
from typing import Optional

import openpyxl
from openpyxl.utils import column_index_from_string


# ---------------------------------------------------------------------------
# CONFIGURACION
# ---------------------------------------------------------------------------

BCRA_URL = (
    "https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/series.xlsm"
)

# La descarga requiere User-Agent de browser y Referer para no ser bloqueada
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bcra.gob.ar/datos-monetarios-diarios/",
    "Accept": "application/vnd.ms-excel,*/*",
}

# Fila donde comienzan los datos en cada hoja (1-indexed, igual en todas las hojas)
DATA_ROW_START = 10

# Catalogo de variables seleccionadas.
# Formato: (id_variable, nombre_corto, hoja_excel, columna_excel, categoria, unidad)
#
# Notas sobre columnas de PRESTAMOS con letra "Q":
#   El catalogo API_Series del BCRA usa "Q " (con espacio). strip() lo normaliza.
VARIABLES: list[tuple] = [

    # ── RESERVAS ─────────────────────────────────────────── (en millones USD)
    (74,  "Reservas - saldo total",                     "RESERVAS",              "C",  "RESERVAS",       "millones USD"),
    (75,  "Reservas - oro, divisas y colocaciones",     "RESERVAS",              "D",  "RESERVAS",       "millones USD"),
    (76,  "Reservas - pase pasivo USD exterior",        "RESERVAS",              "E",  "RESERVAS",       "millones USD"),
    (77,  "Reservas - variacion diaria total",          "RESERVAS",              "G",  "RESERVAS",       "millones USD"),
    (78,  "Reservas - variacion compra divisas",        "RESERVAS",              "H",  "RESERVAS",       "millones USD"),
    (79,  "Reservas - variacion organismos internac",   "RESERVAS",              "I",  "RESERVAS",       "millones USD"),
    (80,  "Reservas - variacion otras op sector pub",   "RESERVAS",              "J",  "RESERVAS",       "millones USD"),
    (81,  "Reservas - variacion efectivo minimo",       "RESERVAS",              "K",  "RESERVAS",       "millones USD"),
    (82,  "Reservas - variacion otros",                 "RESERVAS",              "L",  "RESERVAS",       "millones USD"),
    (83,  "DEGs 2009 - saldo asignaciones",             "RESERVAS",              "N",  "RESERVAS",       "millones USD"),
    (84,  "Tipo de cambio BCRA (pesos por USD)",        "RESERVAS",              "P",  "RESERVAS",       "pesos/USD"),

    # ── BASE MONETARIA ───────────────────────────────────── (en millones $)
    (46,  "BM - factores explicacion variacion",        "BASE MONETARIA",        "C",  "BASE MONETARIA", "millones $"),
    (49,  "BM - adelantos transitorios al Tesoro",      "BASE MONETARIA",        "F",  "BASE MONETARIA", "millones $"),
    (50,  "BM - transferencia utilidades al Tesoro",    "BASE MONETARIA",        "G",  "BASE MONETARIA", "millones $"),
    (64,  "BM - variacion diaria base monetaria",       "BASE MONETARIA",        "V",  "BASE MONETARIA", "millones $"),
    (67,  "BM - saldo billetes y monedas publico",      "BASE MONETARIA",        "Z",  "BASE MONETARIA", "millones $"),
    (68,  "BM - saldo billetes y monedas entidades",    "BASE MONETARIA",        "AA", "BASE MONETARIA", "millones $"),
    (70,  "BM - saldo cuentas corrientes en BCRA",      "BASE MONETARIA",        "AC", "BASE MONETARIA", "millones $"),
    (71,  "BM - saldo base monetaria",                  "BASE MONETARIA",        "AD", "BASE MONETARIA", "millones $"),
    (73,  "BM - saldo base mas cuasimonedas",           "BASE MONETARIA",        "AF", "BASE MONETARIA", "millones $"),
    (109, "M2 - agregado monetario amplio",             "DEPOSITOS",             "AC", "BASE MONETARIA", "millones $"),
    (197, "M2 - transaccional privado",                 "DEPOSITOS",             "AD", "BASE MONETARIA", "millones $"),

    # ── DEPOSITOS ────────────────────────────────────────── (en millones $)
    # Frecuencia: mensual (dato de fin de mes, rezago ~49 dias)
    (94,  "Depositos - CC sector privado",              "DEPOSITOS",             "K",  "DEPOSITOS",      "millones $"),
    (95,  "Depositos - CA sector privado",              "DEPOSITOS",             "L",  "DEPOSITOS",      "millones $"),
    (96,  "Depositos - PF no ajustable priv",           "DEPOSITOS",             "M",  "DEPOSITOS",      "millones $"),
    (97,  "Depositos - PF ajustable CER/UVA priv",      "DEPOSITOS",             "N",  "DEPOSITOS",      "millones $"),
    (102, "Depositos - total pesos sector privado",     "DEPOSITOS",             "S",  "DEPOSITOS",      "millones $"),
    (104, "Depositos - USD privado en pesos",           "DEPOSITOS",             "U",  "DEPOSITOS",      "millones $"),
    (106, "Depositos - total pesos y USD priv en $",    "DEPOSITOS",             "X",  "DEPOSITOS",      "millones $"),
    (108, "Depositos - USD sector privado en USD",      "DEPOSITOS",             "AA", "DEPOSITOS",      "millones USD"),

    # ── PRESTAMOS ────────────────────────────────────────── (en millones)
    # Frecuencia: mensual (dato de fin de mes, rezago ~49 dias)
    (110, "Prestamos - adelantos CC pesos",             "PRESTAMOS",             "B",  "PRESTAMOS",      "millones $"),
    (111, "Prestamos - documentos pesos",               "PRESTAMOS",             "C",  "PRESTAMOS",      "millones $"),
    (112, "Prestamos - hipotecarios pesos",             "PRESTAMOS",             "D",  "PRESTAMOS",      "millones $"),
    (113, "Prestamos - prendarios pesos",               "PRESTAMOS",             "E",  "PRESTAMOS",      "millones $"),
    (114, "Prestamos - personales pesos",               "PRESTAMOS",             "F",  "PRESTAMOS",      "millones $"),
    (115, "Prestamos - tarjetas credito pesos",         "PRESTAMOS",             "G",  "PRESTAMOS",      "millones $"),
    (116, "Prestamos - otros pesos",                    "PRESTAMOS",             "H",  "PRESTAMOS",      "millones $"),
    (117, "Prestamos - total pesos",                    "PRESTAMOS",             "I",  "PRESTAMOS",      "millones $"),
    (125, "Prestamos - total USD en USD",               "PRESTAMOS",             "Q",  "PRESTAMOS",      "millones USD"),
    (127, "Prestamos - total pesos y USD en pesos",     "PRESTAMOS",             "U",  "PRESTAMOS",      "millones $"),

    # ── TASAS DE MERCADO ─────────────────────────────────── (en %)
    # Frecuencia: diaria, ~3 dias de rezago
    (1189, "PF - tasa total general TNA",               "TASAS DE MERCADO",      "B",  "TASAS",          "%"),
    (1190, "PF - tasa personas humanas TNA",            "TASAS DE MERCADO",      "C",  "TASAS",          "%"),
    (138,  "BADLAR - total bancos TNA",                 "TASAS DE MERCADO",      "L",  "TASAS",          "%"),
    (139,  "BADLAR - bancos privados TNA",              "TASAS DE MERCADO",      "M",  "TASAS",          "%"),
    (144,  "Tasa prestamos personales TNA",             "TASAS DE MERCADO",      "R",  "TASAS",          "%"),
    (145,  "Tasa adelantos empresas TNA",               "TASAS DE MERCADO",      "S",  "TASAS",          "%"),

    # ── INSTRUMENTOS DEL BCRA ────────────────────────────── (mixto)
    # Frecuencia: diaria, ~3 dias de rezago
    # Nota: tasa politica monetaria discontinuada como serie separada ~jul 2025
    (160, "Tasa politica monetaria TNA",                "INSTRUMENTOS DEL BCRA", "K",  "TASAS",          "%"),
    (161, "Tasa politica monetaria TEA",                "INSTRUMENTOS DEL BCRA", "L",  "TASAS",          "%"),
    (152, "Pases pasivos - saldo total",                "INSTRUMENTOS DEL BCRA", "B",  "INSTRUMENTOS",   "millones $"),
    (155, "LELIQ y NOTALIQ - saldo",                    "INSTRUMENTOS DEL BCRA", "F",  "INSTRUMENTOS",   "millones $"),
    (196, "LEFI - cartera entidades",                   "INSTRUMENTOS DEL BCRA", "AV", "INSTRUMENTOS",   "millones $"),
]


# ---------------------------------------------------------------------------
# FUNCIONES
# ---------------------------------------------------------------------------

def descargar_xlsx(url: str) -> bytes:
    """
    Descarga el XLSM del BCRA.
    Requiere User-Agent de browser y Referer; sin ellos el servidor devuelve HTML.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=120, context=ctx) as resp:
        contenido = resp.read()

    # Validar que recibimos un archivo Excel (ZIP), no HTML de error
    if contenido[:2] != b"PK":
        raise ValueError(
            f"El servidor devolvio contenido inesperado (no es un archivo ZIP/Excel). "
            f"Primeros bytes: {contenido[:50]}"
        )
    return contenido


def extraer_serie(
    ws: openpyxl.worksheet.worksheet.Worksheet,
    col_letra: str,
    row_start: int = DATA_ROW_START,
) -> list[tuple[str, float]]:
    """
    Extrae pares (fecha_str, valor) de una hoja dado una letra de columna.

    - Ignora filas donde la columna A no es datetime (metadatos, headers).
    - Ignora filas donde el valor no es numerico (celdas vacias, texto).
    - Devuelve fechas en formato ISO 8601 (YYYY-MM-DD).
    """
    col_idx = column_index_from_string(col_letra.strip())

    datos: list[tuple[str, float]] = []
    for row in ws.iter_rows(min_row=row_start, values_only=True):
        fecha_raw = row[0]          # Columna A siempre es la fecha
        valor_raw = row[col_idx - 1]

        if not isinstance(fecha_raw, datetime):
            continue
        if valor_raw is None or not isinstance(valor_raw, (int, float)):
            continue

        datos.append((fecha_raw.strftime("%Y-%m-%d"), float(valor_raw)))

    return datos


def guardar_last_update(variables_info: list[dict], output_dir: str) -> None:
    """
    Guarda un JSON con la ultima fecha disponible por variable y categoria.
    Util para mostrar 'Datos al DD/MM/AAAA' en el dashboard de Power BI.
    """
    resumen = {
        "pipeline_ejecutado_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "variables": variables_info,
    }
    path = os.path.join(output_dir, "bcra_last_update.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# PIPELINE PRINCIPAL
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("Pipeline BCRA")
    print("=" * 60)

    # 1. Descargar el archivo
    print(f"\nDescargando: {BCRA_URL}")
    try:
        contenido = descargar_xlsx(BCRA_URL)
    except Exception as exc:
        print(f"\nERROR al descargar el archivo: {exc}", file=sys.stderr)
        sys.exit(1)
    print(f"Descarga OK: {len(contenido) / 1_000_000:.1f} MB")

    # 2. Abrir el workbook
    wb = openpyxl.load_workbook(
        io.BytesIO(contenido), read_only=True, keep_vba=False
    )
    print(f"Hojas disponibles: {wb.sheetnames}\n")

    # 3. Extraer todas las variables
    hechos: list[tuple[str, int, float]] = []
    variables_info: list[dict] = []
    errores: list[str] = []

    for (id_var, nombre, hoja, col, categoria, unidad) in VARIABLES:

        if hoja not in wb.sheetnames:
            errores.append(
                f"[CRITICO] Hoja '{hoja}' no encontrada en el archivo "
                f"(variable ID {id_var}: {nombre})"
            )
            continue

        ws = wb[hoja]
        try:
            serie = extraer_serie(ws, col)
        except Exception as exc:
            errores.append(
                f"[CRITICO] Error extrayendo ID {id_var} "
                f"(hoja={hoja}, col={col}): {exc}"
            )
            continue

        if not serie:
            errores.append(
                f"[AVISO] ID {id_var} ({nombre}): "
                f"0 filas extraidas de hoja='{hoja}', col='{col}'"
            )
            continue

        for fecha, valor in serie:
            hechos.append((fecha, id_var, valor))

        ultima_fecha = serie[-1][0]
        variables_info.append({
            "id_variable": id_var,
            "nombre": nombre,
            "categoria": categoria,
            "unidad": unidad,
            "ultima_fecha_disponible": ultima_fecha,
            "total_filas": len(serie),
        })

        print(
            f"  ID {id_var:4d} | {len(serie):6,d} filas "
            f"| hasta {ultima_fecha} | {nombre}"
        )

    wb.close()

    # 4. Guardar archivos
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)

    # Tabla de hechos (fact table)
    path_hechos = os.path.join(output_dir, "bcra_hechos.csv")
    with open(path_hechos, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fecha", "id_variable", "valor"])
        writer.writerows(hechos)

    # Tabla de dimension de variables
    path_vars = os.path.join(output_dir, "bcra_variables.csv")
    with open(path_vars, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id_variable", "nombre", "categoria", "unidad"])
        for (id_var, nombre, hoja, col, categoria, unidad) in VARIABLES:
            writer.writerow([id_var, nombre, categoria, unidad])

    # JSON de ultima actualizacion (para el dashboard)
    guardar_last_update(variables_info, output_dir)

    print(f"\nbcra_hechos.csv     : {len(hechos):>10,} filas")
    print(f"bcra_variables.csv  : {len(VARIABLES):>10} variables")
    print(f"bcra_last_update.json: generado")

    # 5. Resultado final
    if errores:
        print("\n" + "=" * 60)
        print("ERRORES / AVISOS DURANTE LA EJECUCION:")
        for e in errores:
            print(f"  {e}", file=sys.stderr)
        print("=" * 60)
        # Exit 1 para que GitHub Actions lo marque como fallido
        # y el usuario reciba notificacion
        sys.exit(1)

    print("\nPipeline BCRA completado exitosamente.")


if __name__ == "__main__":
    main()
