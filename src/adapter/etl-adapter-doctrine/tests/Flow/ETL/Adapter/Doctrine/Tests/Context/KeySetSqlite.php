<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Schema;

use function Flow\ETL\Adapter\Doctrine\to_dbal_schema_table;

final readonly class KeySetSqlite
{
    /**
     * @param list<string> $values
     */
    public static function withCaseInsensitiveKeys(Connection $connection, string $table, array $values): Connection
    {
        $connection
            ->createSchemaManager()
            ->createTable(
                (new Table($table, [
                    new Column('id', Type::getType(Types::INTEGER)),
                    new Column('k', Type::getType(Types::STRING), [
                        'length' => 10,
                        'platformOptions' => ['collation' => 'NOCASE'],
                    ]),
                ]))
                    ->edit()
                    ->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create())
                    ->create(),
            );

        foreach ($values as $index => $value) {
            $connection->insert($table, ['id' => $index + 1, 'k' => $value]);
        }

        return $connection;
    }

    /**
     * @param list<array<string, mixed>> $rows
     */
    public static function withKeys(Connection $connection, string $table, Schema $schema, array $rows): Connection
    {
        $connection->createSchemaManager()->createTable(to_dbal_schema_table($schema, $table));

        foreach ($rows as $row) {
            $connection->insert($table, $row);
        }

        return $connection;
    }
}
