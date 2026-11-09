{
    lib,
    writeShellScriptBin,
    writeShellScript,
    _1password-cli,
    gh,
    blackfire,
    composer,
}:

let
    with-op-env = name: { program, op-program ? program, refs, skip-if-set ? [ ] }: writeShellScriptBin name ''
        if [ -n "''${FLOW_OP_ENV_FILE:-}" ] && [ -f "$FLOW_OP_ENV_FILE" ]; then
            . "$FLOW_OP_ENV_FILE"
            if [ -n "''${OP_VAULT:-}" ]${lib.concatMapStrings (variable: " && [ -z \"\${${variable}:-}\" ]") skip-if-set}; then
                if [ -n "''${OP_ACCOUNT:-}" ]; then export OP_ACCOUNT; fi
                # macOS 27 can block op from the 1Password app's settings file, then op reports no accounts
                export OP_BIOMETRIC_UNLOCK_ENABLED="''${OP_BIOMETRIC_UNLOCK_ENABLED:-true}"
                ${lib.concatStringsSep "\n        " (lib.mapAttrsToList (variable: reference: ''export ${variable}="op://''${OP_VAULT}/${reference}"'') refs)}
                exec ${_1password-cli}/bin/op run --no-masking -- ${op-program} "$@"
            fi
        fi
        exec ${program} "$@"
    '';

    composer-github-auth = writeShellScript "composer-github-auth" ''
        export COMPOSER_AUTH="{\"github-oauth\":{\"github.com\":\"$GH_TOKEN\"}}"
        unset GH_TOKEN
        exec ${composer}/bin/composer "$@"
    '';
in
{
    gh = with-op-env "gh" {
        program = "${gh}/bin/gh";
        refs = { GH_TOKEN = "github-gh-cli/credential"; };
    };

    composer = with-op-env "composer" {
        program = "${composer}/bin/composer";
        op-program = "${composer-github-auth}";
        skip-if-set = [ "COMPOSER_AUTH" ];
        refs = { GH_TOKEN = "github-gh-cli/credential"; };
    };

    blackfire = with-op-env "blackfire" {
        program = "${blackfire}/bin/blackfire";
        refs = {
            BLACKFIRE_CLIENT_ID = "blackfire-client/client-id";
            BLACKFIRE_CLIENT_TOKEN = "blackfire-client/credential";
        };
    };
}
