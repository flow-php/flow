<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Pagination\{Key, KeySet, Order};
use Flow\ETL\{Attribute\DocumentationDSL, Attribute\Module, Attribute\Type as DSLType};
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::EXTRACTOR)]
function from_pgsql_limit_offset(
    Client $client,
    string|SqlQuery $query,
    int $pageSize = 1000,
    ?int $maximum = null,
) : PostgreSqlLimitOffsetExtractor {
    $extractor = (new PostgreSqlLimitOffsetExtractor($client, $query))
        ->withPageSize($pageSize);

    if ($maximum !== null) {
        $extractor->withMaximum($maximum);
    }

    return $extractor;
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::EXTRACTOR)]
function from_pgsql_key_set(
    Client $client,
    string|SqlQuery $query,
    KeySet $keySet,
    int $pageSize = 1000,
    ?int $maximum = null,
) : PostgreSqlKeySetExtractor {
    $extractor = (new PostgreSqlKeySetExtractor($client, $query, $keySet))
        ->withPageSize($pageSize);

    if ($maximum !== null) {
        $extractor->withMaximum($maximum);
    }

    return $extractor;
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_pagination_key_asc(string $column) : Key
{
    return new Key($column, Order::ASC);
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_pagination_key_desc(string $column) : Key
{
    return new Key($column, Order::DESC);
}

#[DocumentationDSL(module: Module::POSTGRESQL, type: DSLType::HELPER)]
function pgsql_pagination_key_set(Key ...$keys) : KeySet
{
    return new KeySet(...$keys);
}
