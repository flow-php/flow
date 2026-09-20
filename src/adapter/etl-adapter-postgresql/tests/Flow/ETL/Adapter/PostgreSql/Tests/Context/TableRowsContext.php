<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Context;

use Flow\PostgreSql\Client\Client;

use function array_values;
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;
use function Flow\ETL\DSL\df;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class TableRowsContext
{
    /**
     * @return array<int, array<array-key, mixed>>
     */
    public static function fetchAll(Client $client, string $table): array
    {
        return df()
            ->read(from_pgsql_limit_offset($client, select(star())->from(table($table))->orderBy(asc(col('id')))))
            ->fetch()
            ->toArray();
    }

    /**
     * Rows a (sub)transaction wrote carry its xid as `xmin`, so one group is one committed transaction.
     *
     * @return list<list<int>> the values of $column, grouped by the transaction that wrote them, ordered by $column
     */
    public static function groupedByWritingTransaction(Client $client, string $table, string $column): array
    {
        $groups = [];

        foreach ($client->fetchAll(
            select(cast(col('xmin'), column_type_text())->as('xmin'), col($column))
                ->from(table($table))
                ->orderBy(asc(col($column))),
        ) as $row) {
            $groups[type_string()->assert($row['xmin'])][] = type_integer()->assert($row[$column]);
        }

        return array_values($groups);
    }
}
