<?php

declare(strict_types=1);

namespace Flow\PgQuery\DSL;

use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type as DSLType;

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_parser() : Parser
{
    return new Parser();
}

#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_parse(string $sql) : ParsedQuery
{
    return (new Parser())->parse($sql);
}

/**
 * Returns a fingerprint of the given SQL query.
 * Literal values are normalized so they won't affect the fingerprint.
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_fingerprint(string $sql) : ?string
{
    return (new Parser())->fingerprint($sql);
}

/**
 * Normalize SQL query by replacing literal values and named parameters with positional parameters.
 * WHERE id = :id will be changed into WHERE id = $1
 * WHERE id = 1 will be changed into WHERE id = $1
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_normalize(string $sql) : ?string
{
    return (new Parser())->normalize($sql);
}

/**
 * Split string with multiple SQL statements into array of individual statements.
 *
 * @return array<string>
 */
#[DocumentationDSL(module: Module::PG_QUERY, type: DSLType::HELPER)]
function pg_split(string $sql) : array
{
    return (new Parser())->split($sql);
}
