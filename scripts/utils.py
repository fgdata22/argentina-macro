"""
utils.py — funciones compartidas para todos los pipelines del proyecto.

Importar con:
    from utils import descargar_archivo, escribir_csv, escribir_json
"""

import csv
import json
import ssl
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Descarga
# ---------------------------------------------------------------------------

# Headers que simulan un navegador real (necesario para BCRA y algunos servidores
# del gobierno que bloquean User-Agents de scripts).
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}


def descargar_archivo(url: str, headers_extra: dict | None = None, timeout: int = 60) -> bytes:
    """
    Descarga un archivo desde `url` y devuelve su contenido como bytes.

    - Omite la verificación SSL (servidores del gobierno argentino suelen
      tener cadenas de certificados desactualizadas).
    - Levanta RuntimeError si el servidor devuelve un código != 200 o si
      el contenido parece una página HTML en lugar del archivo esperado.
    """
    headers = {**BROWSER_HEADERS, **(headers_extra or {})}
    req = urllib.request.Request(url, headers=headers)

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
            contenido = resp.read()
    except Exception as exc:
        raise RuntimeError(f"Error descargando {url}: {exc}") from exc

    # Detectar si el servidor devolvió HTML en lugar del archivo
    if contenido[:5].lower().startswith(b"<!doc") or b"<html" in contenido[:200].lower():
        raise RuntimeError(
            f"El servidor devolvio HTML en lugar del archivo esperado. URL: {url}"
        )

    return contenido


# ---------------------------------------------------------------------------
# Escritura de CSV
# ---------------------------------------------------------------------------

def escribir_csv(path: str | Path, filas: list[dict], columnas: list[str]) -> int:
    """
    Escribe `filas` (lista de dicts) en un CSV con las columnas indicadas.

    - Usa punto y coma como separador para compatibilidad con Excel en español.
    - Valores None o ausentes se escriben como cadena vacía (= NULL en Power BI).
    - Devuelve el número de filas escritas (sin contar el header).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=columnas,
            delimiter=";",
            extrasaction="ignore",
            restval="",
        )
        writer.writeheader()
        writer.writerows(filas)

    return len(filas)


# ---------------------------------------------------------------------------
# Escritura de JSON de metadata
# ---------------------------------------------------------------------------

def escribir_json(path: str | Path, data: dict) -> None:
    """Escribe `data` como JSON con indentación legible."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


# ---------------------------------------------------------------------------
# Helpers de fecha
# ---------------------------------------------------------------------------

MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def mes_nombre_a_numero(nombre: str) -> int | None:
    """Convierte 'Enero' / 'enero' / 'ENERO' a 1. Devuelve None si no reconoce."""
    return MESES_ES.get(nombre.strip().lower())


def periodo_yyyymm_a_fecha(periodo: str) -> str | None:
    """
    Convierte '202602' → '2026-02-01'.
    Devuelve None si el formato no es válido.
    """
    periodo = str(periodo).strip()
    if len(periodo) == 6 and periodo.isdigit():
        return f"{periodo[:4]}-{periodo[4:6]}-01"
    return None


def timestamp_utc() -> str:
    """Devuelve el timestamp actual en UTC como string ISO 8601."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Logging simple (sin dependencias externas)
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    """Imprime un mensaje con timestamp UTC. Va al stdout de GitHub Actions."""
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def log_error(msg: str) -> None:
    """Imprime un error en stderr."""
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] ERROR: {msg}", file=sys.stderr, flush=True)
