#!/bin/bash

MAX_SCREENS=${1:-6}

for SCREEN_NUM in $(seq 1 $MAX_SCREENS); do
    SCREEN_NAME="fetchtwitch${SCREEN_NUM}"

    screen -dmS "$SCREEN_NAME" bash -c "START_TIME=\$(date +%s); uv run scripts/twitchtracker/fetch_twitchtracker_game.py $SCREEN_NUM $MAX_SCREENS; END_TIME=\$(date +%s); ELAPSED_TIME=\$((END_TIME - START_TIME)); echo \"Temps d'exécution: \$ELAPSED_TIME secondes\"; exec bash"

    echo "Screen $SCREEN_NAME lancé pour l'instance $SCREEN_NUM"
    sleep 30
done
echo "Pour attacher le screen : screen -r {num du screen}"
echo "Pour lister les screens : screen -ls"
