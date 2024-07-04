#!/bin/bash

# Encontra o PID do processo Python3
PID=$(pgrep -f "python3 webserver/webserver.py")

# Verifica se o PID foi encontrado
if [ -n "$PID" ]; then
    # Encerra o processo
    kill "$PID"
    echo "Webhook encerrado."
else
    echo "Processo do Webhook não encontrado"
fi