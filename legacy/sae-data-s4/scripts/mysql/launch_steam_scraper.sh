#!/bin/bash

for SCREEN_NUM in {1..6}; do
    SCREEN_NAME="fetchsteam${SCREEN_NUM}"

    screen -dmS "$SCREEN_NAME" bash -c "START_TIME=\$(date +%s); uv run fetch_steam_games.py $SCREEN_NUM; END_TIME=\$(date +%s); ELAPSED_TIME=\$((END_TIME - START_TIME)); echo \"Temps d'exécution: \$ELAPSED_TIME secondes\"; exec bash"

    echo "Screen $SCREEN_NAME lancé pour l'instance $SCREEN_NUM"
    echo "Pour attacher le screen : screen -r $SCREEN_NAME"
    echo "Pour lister les screens : screen -ls"
done
