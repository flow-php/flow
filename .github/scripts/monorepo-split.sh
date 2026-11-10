#!/usr/bin/env bash
set -euo pipefail

local_path="$1"
split_repository="$2"

remote="https://github.com/flow-php/${split_repository}.git"
helper="!f() { echo username=x-access-token; echo \"password=\${SPLIT_TOKEN}\"; }; f"
git_auth() { git -c credential.helper= -c credential.helper="$helper" "$@"; }
git_author() { git -c user.name="$BOT_NAME" -c user.email="$BOT_EMAIL" "$@"; }

message="$(git show -s --format=%B "$GITHUB_SHA")"
message="${message%%$'\n'*}"
attempt=1

while true; do
  build="$(mktemp -d)"
  heads="$(git_auth ls-remote --heads "$remote" 1.x)"

  if [ -n "$heads" ]; then
    git_auth clone --quiet --depth 1 --branch 1.x "$remote" "$build"
  else
    echo "flow-php/${split_repository} has no 1.x branch, starting it as an orphan."
    git -C "$build" init --quiet
    git -C "$build" remote add origin "$remote"
    git -C "$build" checkout --quiet --orphan 1.x
  fi

  find "$build" -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +
  cp -a "${local_path}/." "$build/"
  git -C "$build" add --all
  changes="$(git -C "$build" status --porcelain)"

  if [ -z "$changes" ]; then
    echo "flow-php/${split_repository} 1.x is up to date."
    break
  fi

  git_author -C "$build" commit --quiet --message "$message"
  push_exit=0
  push_status="$(git_auth -C "$build" push --porcelain origin 1.x)" || push_exit=$?

  if [ "$push_exit" -eq 0 ]; then
    break
  fi

  echo "$push_status"

  if [ "$attempt" -eq 3 ] || ! grep -qF '[rejected]' <<< "$push_status"; then
    exit "$push_exit"
  fi

  echo "flow-php/${split_repository} 1.x moved during the split, attempt $((attempt + 1)) of 3."
  rm -rf "$build"
  attempt=$((attempt + 1))
done

if [ "$GITHUB_REF_TYPE" = "tag" ]; then
  git_author -C "$build" tag -a "$GITHUB_REF_NAME" -m "Publishing $GITHUB_REF_NAME"
  git_auth -C "$build" push --quiet origin "$GITHUB_REF_NAME"
fi

rm -rf "$build"
