"""
Ponte entre o arquivo bruto (que cresce para sempre, gerado pelo
gerador.py) e o Flink (cujo FileSource.monitor_continuously() só detecta
ARQUIVOS NOVOS aparecendo num diretório -- ele não observa um arquivo já
existente crescer; ver https://www.mail-archive.com/user@flink.apache.org/msg52152.html).

Este processo lê o que foi adicionado ao arquivo bruto desde a última
leitura, escreve num arquivo de staging (.tmp) e só então o renomeia para
o nome final dentro do diretório "ready/". O rename é atômico no mesmo
filesystem, então o Flink nunca enxerga um arquivo pela metade: quando ele
aparece, já está completo.
"""

import os
import time
import uuid
from pathlib import Path

SOURCE_FILE = os.getenv("EVENT_FILE", "/data/events.jsonl")
READY_DIR = os.getenv("FLINK_READY_DIR", "/ready")
ROTATE_INTERVAL_SECONDS = float(os.getenv("ROTATE_INTERVAL_SECONDS", "5"))
STATE_FILE = os.getenv("ROTATOR_STATE_FILE", "/state/rotator_offset.txt")

Path(READY_DIR).mkdir(parents=True, exist_ok=True)
Path(os.path.dirname(STATE_FILE)).mkdir(parents=True, exist_ok=True)


def read_offset():
    try:
        with open(STATE_FILE) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0


def write_offset(offset):
    with open(STATE_FILE, "w") as f:
        f.write(str(offset))


offset = read_offset()
print(
    f"Rotator iniciado. Lendo {SOURCE_FILE} a partir do offset {offset}, "
    f"gravando lotes prontos em {READY_DIR} a cada {ROTATE_INTERVAL_SECONDS}s",
    flush=True,
)

while True:
    time.sleep(ROTATE_INTERVAL_SECONDS)

    if not os.path.exists(SOURCE_FILE):
        continue

    size = os.path.getsize(SOURCE_FILE)
    if size < offset:
        # arquivo foi truncado/recriado (ex.: reinício do laboratório do zero)
        offset = 0
    if size <= offset:
        continue

    with open(SOURCE_FILE, "rb") as f:
        f.seek(offset)
        chunk = f.read(size - offset)

    # avança só até a última quebra de linha completa, para nunca cortar
    # um evento JSON no meio caso o gerador esteja escrevendo naquele
    # instante.
    last_newline = chunk.rfind(b"\n")
    if last_newline == -1:
        continue

    complete_chunk = chunk[: last_newline + 1]
    new_offset = offset + len(complete_chunk)

    batch_name = f"events-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}.jsonl"
    # Prefixo "." faz o enumerador padrão do Flink ignorar este arquivo
    # mesmo se, por azar, uma varredura acontecer bem entre a escrita e o
    # rename (o enumerador padrão do FileSource filtra arquivos ocultos --
    # nomes começando com "." ou "_"). O rename para o nome final, sem
    # ponto, é o que o torna visível -- e é atômico, então nunca existe um
    # arquivo com o nome final parcialmente escrito.
    staging_path = os.path.join(READY_DIR, "." + batch_name + ".tmp")
    ready_path = os.path.join(READY_DIR, batch_name)

    with open(staging_path, "wb") as f:
        f.write(complete_chunk)

    os.rename(staging_path, ready_path)  # atômico no mesmo filesystem

    offset = new_offset
    write_offset(offset)
    print(f"Lote pronto: {ready_path} ({len(complete_chunk)} bytes)", flush=True)
