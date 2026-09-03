<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Context;

use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnDefinition;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

use function array_map;
use function Flow\PostgreSql\DSL\create;

final class DescribeProbeContext
{
    /**
     * A temporary table covering the types a describe answer has to report. Temporary tables are
     * dropped with the session, so no teardown is needed.
     */
    public static function createTable(Client $client, string $table): void
    {
        $client->execute(
            create()
                ->temporaryTable($table)
                ->column(ColumnDefinition::create('id', ColumnType::bigint()))
                ->column(ColumnDefinition::create('label', ColumnType::text()))
                ->column(ColumnDefinition::create('amount', ColumnType::numeric(10, 2)))
                ->column(ColumnDefinition::create('flag', ColumnType::boolean()))
                ->column(ColumnDefinition::create('day', ColumnType::date()))
                ->column(ColumnDefinition::create('clock', ColumnType::time()))
                ->column(ColumnDefinition::create('moment', ColumnType::timestamptz()))
                ->column(ColumnDefinition::create('identifier', ColumnType::uuid()))
                ->column(ColumnDefinition::create('document', ColumnType::jsonb()))
                ->column(ColumnDefinition::create('markup', ColumnType::xml())),
        );
    }

    /**
     * @param list<array{name: string, type: ColumnType}> $columns
     *
     * @return list<string>
     */
    public static function typeNames(array $columns): array
    {
        return array_map(static fn(array $column): string => $column['type']->normalize()['name'], $columns);
    }
}
