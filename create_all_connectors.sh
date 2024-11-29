#!/bin/bash

# Diretório onde estão os arquivos JSON
CONFIG_DIR="./config"

# URL do Kafka Connect
CONNECT_URL="http://connect:8083/connectors"

# Iterar por todos os arquivos JSON e enviá-los ao Kafka Connect
for file in "$CONFIG_DIR"/*.json; do
  echo "Enviando $file para Kafka Connect"
  curl -X POST -H "Content-Type: application/json" --data @"$file" "$CONNECT_URL"
  echo -e "\nConector enviado: $file\n"
done