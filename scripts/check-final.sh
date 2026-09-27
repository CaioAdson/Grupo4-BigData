#!/usr/bin/env bash
# Verificação da entrega final. Ao contrário da primeira versão, este
# script FALHA (exit != 0) se alguma etapa não estiver realmente
# funcionando -- ele não apenas imprime o comando e segue em frente.
set -uo pipefail
FAIL=0

check() {
  local desc="$1"
  if [ "$2" -eq 0 ]; then
    echo "[OK] $desc"
  else
    echo "[FALHOU] $desc"
    FAIL=1
  fi
}

echo "=== [1/6] Validando o Compose ==="
docker compose config --quiet
check "docker-compose.yml válido" $?

echo
echo "=== [2/6] Conferindo serviços ativos ==="
docker compose ps
NAO_ATIVOS=$(docker compose ps --status=running --format '{{.Name}}' | wc -l)
echo "Serviços em execução: $NAO_ATIVOS"

echo
echo "=== [3/6] Gerador produzindo eventos ==="
docker compose logs --tail=3 generator
check "gerador com logs recentes" $?

echo
echo "=== [4/6] Flume + HDFS: arquivos realmente gravados ==="
ARQUIVOS_HDFS=$(docker compose exec -T namenode hdfs dfs -find /ecommerce/raw -name "events-*" 2>/dev/null | wc -l)
echo "Arquivos encontrados em /ecommerce/raw: $ARQUIVOS_HDFS"
if [ "$ARQUIVOS_HDFS" -gt 0 ]; then
  check "Flume gravando arquivos no HDFS" 0
else
  check "Flume gravando arquivos no HDFS (nenhum arquivo encontrado -- espere o rollInterval de 60s e rode de novo)" 1
fi

echo
echo "=== [5/6] Flink: job rodando (não deve estar FINISHED) ==="
JOBS_JSON=$(curl -s http://localhost:8081/jobs || echo "")
if echo "$JOBS_JSON" | grep -q '"status":"RUNNING"'; then
  check "job do Flink em execução" 0
else
  echo "Resposta da API do Flink: $JOBS_JSON"
  check "job do Flink em execução" 1
fi

echo
echo "=== [6/6] HBase: tabela existe e responde ==="
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8090/product_alerts/schema || echo "000")
if [ "$HTTP_STATUS" = "200" ]; then
  check "tabela product_alerts acessível via REST" 0
else
  echo "HTTP status recebido: $HTTP_STATUS"
  check "tabela product_alerts acessível via REST" 1
fi

echo
echo "=== Extra: Hive Metastore (se o serviço estiver no compose) ==="
if docker compose ps hive-metastore --status=running --format '{{.Name}}' 2>/dev/null | grep -q .; then
  if (exec 3<>/dev/tcp/localhost/9083) 2>/dev/null; then
    exec 3<&- 3>&-
    echo "[OK] porta 9083 do hive-metastore respondendo"
  else
    echo "[FALHOU] hive-metastore está no compose mas a porta 9083 não respondeu ainda"
    FAIL=1
  fi
else
  echo "(serviço hive-metastore não está no compose -- usando catálogo embutido do Spark, ok se foi uma decisão consciente)"
fi
echo
echo "Rode manualmente e confira se a tabela retorna linhas:"
echo "  docker compose exec spark-master spark-sql -e \"SELECT * FROM ecommerce.daily_sales LIMIT 5;\""

echo
if [ "$FAIL" -eq 0 ]; then
  echo "Checkpoint final: tudo OK."
else
  echo "Checkpoint final: HÁ ITENS FALHANDO acima. Não grave o vídeo ainda."
  exit 1
fi
