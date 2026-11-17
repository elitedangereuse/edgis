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
        # Gitignored staging-only files the git-derived list below can't see.
        EXTRA_FILES=(
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
    # Ship what git ships: tracked files plus untracked-but-not-ignored ones,
    # so a NEW file deploys without being hand-listed (the old hardcoded
    # allowlist silently dropped new files and caused live regressions).
    # Exclusions:
    #   - tests/, cicd/, tools/: not needed at runtime
    #   - edts: submodule, deployed manually on the rare occasion it changes
    #   - gitignored files are absent by construction. That keeps secrets
    #     (.env) off the ship list AND keeps the native Rust kernel
    #     (edgis_route.abi3.so) a deliberate build-on-prod step
    #     (rust/build.sh): routing.py falls back to Python when it is absent,
    #     and a cross-built .so silently flipping the engine was a real
    #     incident.
    git -c core.quotepath=off ls-files --cached --others --exclude-standard |
        grep -Ev '^(tests/|cicd/|tools/|edts($|/)|Makefile$|\.coveragerc$)' |
        while IFS= read -r f; do
            # Tracked-but-deleted files would abort rsync before the restart.
            if [[ -f "$f" ]]; then printf '%s\n' "$f"; fi
        done
    # Extra files may be untracked (index-staging.html is gitignored): skip the
    # missing ones so rsync does not abort the deploy before the restart.
    for file in "${EXTRA_FILES[@]}"; do
        if [[ -z "$file" ]]; then
            continue
        fi
        if [[ -f "$file" ]]; then
            echo "$file"
        else
            echo "Skipping missing $file" >&2
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
