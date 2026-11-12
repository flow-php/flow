{
    lib,
    writeShellScriptBin,
    writeShellScript,
    fnox,
    gh,
    blackfire,
    composer,
}:

let
    with-secrets = name: { program, secrets, skip-if-set ? [ ] }: writeShellScriptBin name ''
        ${lib.concatMapStrings (variable: "if [ -n \"\${${variable}:-}\" ]; then exec ${program} \"$@\"; fi\n") skip-if-set}exec ${fnox}/bin/fnox exec --replace --only ${lib.concatStringsSep "," secrets} -- ${program} "$@"
    '';

    composer-github-auth = writeShellScript "composer-github-auth" ''
        if [ -n "''${GH_TOKEN:-}" ] && [ -z "''${COMPOSER_AUTH:-}" ]; then
            export COMPOSER_AUTH="{\"github-oauth\":{\"github.com\":\"$GH_TOKEN\"}}"
        fi
        exec ${composer}/bin/composer "$@"
    '';
in
{
    gh = with-secrets "gh" {
        program = "${gh}/bin/gh";
        secrets = [ "GH_TOKEN" ];
    };

    composer = with-secrets "composer" {
        program = "${composer-github-auth}";
        secrets = [ "GH_TOKEN" ];
        skip-if-set = [ "COMPOSER_AUTH" ];
    };

    blackfire = with-secrets "blackfire" {
        program = "${blackfire}/bin/blackfire";
        secrets = [ "BLACKFIRE_CLIENT_ID" "BLACKFIRE_CLIENT_TOKEN" ];
    };
}
