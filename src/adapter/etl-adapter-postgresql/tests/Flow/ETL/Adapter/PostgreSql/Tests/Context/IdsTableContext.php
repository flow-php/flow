<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Context;

use Flow\PostgreSql\Client\Client;

use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;

final class IdsTableContext
{
    public static function create(Client $client, string $table, int $rows): string
    {
        $client->execute(create()->table($table)->column(column('id', column_type_integer())->primaryKey()));

        $insert = insert()->into($table)->columns('id');

        for ($id = 1; $id <= $rows; $id++) {
            $insert = $insert->values(literal($id));
        }

        $client->execute($insert);

        return $table;
    }
}
