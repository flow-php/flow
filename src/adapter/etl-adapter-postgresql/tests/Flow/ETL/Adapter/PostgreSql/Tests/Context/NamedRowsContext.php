<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Context;

use Flow\PostgreSql\Client\Client;

use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function sprintf;

final class NamedRowsContext
{
    public static function insert(Client $client, string $table, int $count): void
    {
        $insert = insert()->into($table)->columns('id', 'name');

        for ($i = 1; $i <= $count; $i++) {
            $insert = $insert->values(literal($i), literal(sprintf('User_%02d', $i)));
        }

        $client->execute($insert);
    }
}
