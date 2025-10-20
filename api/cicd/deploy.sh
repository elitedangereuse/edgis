#!/bin/bash
set -eu

# Rsync exclude patterns
EXCLUDES=(
    ".github"
    ".git"
    "Makefile"
    "*.sh"
)
TMP_CHANGED_FILE_LIST=$(mktemp)
echo systems.py > "$TMP_CHANGED_FILE_LIST"
echo static/index.html >> "$TMP_CHANGED_FILE_LIST"

# source environment variables
source .env
# SSH command with custom port
SSH_CMD="ssh -p $REMOTE_PORT"
# Sync the changed files (add or modify)
echo "Deploying changed files to $REMOTE_PATH"
rsync -avuP "${EXCLUDE_FLAGS[@]}" --files-from="$TMP_CHANGED_FILE_LIST" -e "$SSH_CMD" \
      --rsync-path="sudo -u $REMOTE_USER rsync" ./ "$REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH"
$SSH_CMD "$REMOTE_USER@$REMOTE_HOST" sudo systemctl restart edgis.service
$SSH_CMD "$REMOTE_USER@$REMOTE_HOST" systemctl status edgis.service
echo
