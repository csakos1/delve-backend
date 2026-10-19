#!/usr/bin/env bash
# Install a commit of this checkout as a release under /opt/delve and switch to it.
#
#   sudo deploy/install.sh [REV]        # default: HEAD of this checkout
#
# One release = /opt/delve/releases/<sha>/{venv,alembic.ini,migrations,REVISION}.
# Steps: build the venv from `git archive REV` (non-editable), run `alembic upgrade head`
# as the `delve` user with /etc/delve/env, point /opt/delve/current at the release,
# restart delve-backend.service if it is enabled, wait for /api/v1/health.
# Rollback: sudo deploy/install.sh <older-sha> (its venv is reused; migrations are not
# downgraded, so roll back only across releases without a new migration).
# Needs: the `delve` user (sysusers), /etc/delve/env, network for PyPI.
set -euo pipefail

BASE=/opt/delve
ENV_FILE=/etc/delve/env
KEEP=3
PYTHON=${PYTHON:-/usr/bin/python3}
REPO=$(cd "$(dirname "$0")/.." && pwd)
REV=${1:-HEAD}

die() { echo "install: $*" >&2; exit 1; }
[ "$(id -u)" -eq 0 ] || die "run with sudo"
id delve >/dev/null 2>&1 || die "no 'delve' user (systemd-sysusers, see delve-infra)"
[ -r "$ENV_FILE" ] || die "missing $ENV_FILE (see deploy/delve.env.example)"

git_() { git -c safe.directory="$REPO" -C "$REPO" "$@"; }
SHA=$(git_ rev-parse --short=12 "$REV^{commit}") || die "unknown revision: $REV"
if [ "$REV" = HEAD ] && [ -n "$(git_ status --porcelain --untracked-files=no)" ]; then
  echo "install: note: uncommitted changes are not installed (only $SHA)" >&2
fi
REL=$BASE/releases/$SHA

install -d -m 0750 -o root -g delve "$BASE" "$BASE/releases"
if [ ! -f "$REL/REVISION" ]; then  # REVISION is written last: its absence = incomplete
  echo "install: building $SHA"
  rm -rf "$REL"
  src=$(mktemp -d); trap 'rm -rf "$src"' EXIT
  git_ archive "$SHA" | tar -x -C "$src"
  mkdir -p "$REL"
  "$PYTHON" -m venv "$REL/venv"  # venvs are not relocatable: build under the final path
  "$REL/venv/bin/pip" install --quiet --no-cache-dir --disable-pip-version-check "$src"
  cp -r "$src/alembic.ini" "$src/migrations" "$REL/"
  echo "$SHA" > "$REL/REVISION"
fi
chown -R root:delve "$REL"
chmod -R u=rwX,g=rX,o= "$REL"

echo "install: migrating"
sudo -u delve sh -c 'set -a; . "$1"; set +a; cd "$2" && exec ./venv/bin/alembic -c alembic.ini upgrade head' \
  sh "$ENV_FILE" "$REL"

ln -sfn "releases/$SHA" "$BASE/current.new"
mv -T "$BASE/current.new" "$BASE/current"
install -m 0755 -o root -g root "$REPO/deploy/delve" /usr/local/bin/delve
echo "install: current -> $SHA"

if systemctl is-enabled --quiet delve-backend.service 2>/dev/null; then
  systemctl restart delve-backend.service
  for _ in $(seq 1 30); do
    if curl -sf -m 2 http://127.0.0.1:8700/api/v1/health >/dev/null; then
      echo "install: delve-backend healthy"
      break
    fi
    sleep 1
  done
  curl -sf -m 2 http://127.0.0.1:8700/api/v1/health >/dev/null \
    || die "not healthy after 30 s: journalctl -u delve-backend -n 50"
else
  echo "install: delve-backend.service is not enabled; not restarted"
fi

# keep the newest $KEEP releases besides the current one
cur=$(readlink "$BASE/current")
ls -1t "$BASE/releases" | grep -v -x -F "${cur#releases/}" | tail -n +"$KEEP" \
  | while read -r old; do rm -rf "${BASE:?}/releases/$old"; done
