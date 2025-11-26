<?php

declare(strict_types=1);

namespace Flow\PgQuery\DSL;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\ParseResult;

function pg_parser() : Parser
{
    return new Parser();
}

function pg_parse(string $sql) : ParseResult
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
