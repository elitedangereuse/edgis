#!/bin/bash
set -euo pipefail

# source environment variables
# shellcheck source=/dev/null
source .env

TARGET="${1:-prod}"
case "$TARGET" in
    prod)
        TARGET_NAME="production"
        TARGET_USER="$REMOTE_USER"
        TARGET_HOST="$REMOTE_HOST"
        TARGET_PATH="$REMOTE_PATH"
        TARGET_PORT="$REMOTE_PORT"
        TARGET_SERVICE="${REMOTE_SERVICE_PROD:-edgis.service}"
        EXTRA_FILES=()
        ;;
    staging)
        TARGET_NAME="staging"
        TARGET_USER="${REMOTE_USER_STAGING:-$REMOTE_USER}"
        TARGET_HOST="${REMOTE_HOST_STAGING:-$REMOTE_HOST}"
        TARGET_PATH="${REMOTE_PATH_STAGING:-$REMOTE_PATH}"
        TARGET_PORT="${REMOTE_PORT_STAGING:-$REMOTE_PORT}"
        TARGET_SERVICE="${REMOTE_SERVICE_STAGING:-edgis-staging.service}"
        EXTRA_FILES=(
            systems_staging.py
            static/index-staging.html
        )
        ;;
    *)
        echo "Unknown deploy target: $TARGET" >&2
        exit 1
        ;;
esac

TMP_CHANGED_FILE_LIST=$(mktemp)
{
    echo systems.py
    echo static/index.html
    echo static/galaxymap.html
    echo static/sysmap.html
    echo static/tailwind.css
    echo static/milkyway.css
    echo static/js/panzoom.js
    echo static/js/ed3dmap.js
    echo static/js/galaxymap.js
    echo static/js/sysmap.js
    echo static/ed3d/main.css
    echo static/ed3d/styles.css
    find static/ed3d/vendor -type f | sed 's#^#./#' | sed 's#^./##'
    find static/ed3d/js -type f | sed 's#^#./#' | sed 's#^./##'
    find static/ed3d/textures -type f | sed 's#^#./#' | sed 's#^./##'
    for file in "${EXTRA_FILES[@]}"; do
        if [[ -n "$file" ]]; then
            echo "$file"
        fi
    done
} > "$TMP_CHANGED_FILE_LIST"

SSH_CMD=(ssh -p "$TARGET_PORT")

echo "Deploying changed files to $TARGET_NAME [$TARGET_USER@$TARGET_HOST:$TARGET_PATH]"
rsync -avuP --files-from="$TMP_CHANGED_FILE_LIST" -e "${SSH_CMD[*]}" \
      --rsync-path="sudo -u $TARGET_USER rsync" ./ "$TARGET_USER@$TARGET_HOST:$TARGET_PATH"

"${SSH_CMD[@]}" "$TARGET_USER@$TARGET_HOST" sudo systemctl restart "$TARGET_SERVICE"
"${SSH_CMD[@]}" "$TARGET_USER@$TARGET_HOST" systemctl status "$TARGET_SERVICE"
echo
