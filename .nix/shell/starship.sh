#!/usr/bin/env bash

# Directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if starship.toml exists, otherwise use starship.toml.dist
if [ -f "$SCRIPT_DIR/starship.toml" ]; then
  export STARSHIP_CONFIG="$SCRIPT_DIR/starship.toml"
else
  export STARSHIP_CONFIG="$SCRIPT_DIR/starship.toml.dist"
fi

# Initialize Starship prompt
eval "$(${pkgs.starship}/bin/starship init bash)"