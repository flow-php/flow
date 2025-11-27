<?php

declare(strict_types=1);

namespace Flow\PgQuery\DSL;

use Flow\PgQuery\{ParsedQuery, Parser};

function pg_parser() : Parser
{
    return new Parser();
}

function pg_parse(string $sql) : ParsedQuery
{
    return (new Parser())->parse($sql);
}

function pg_fingerprint(string $sql) : ?string
{
    return (new Parser())->fingerprint($sql);
}

function pg_normalize(string $sql) : ?string
{
    return (new Parser())->normalize($sql);
}

/**
 * @return array<string>
 */
function pg_split(string $sql) : array
{
    return (new Parser())->split($sql);
}
