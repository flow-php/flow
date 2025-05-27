<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\Doctrine\Bulk\Dialect\PostgreSQLInsertOptions;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Exception\InvalidArgumentException;

final class DbalLoaderTest extends IntegrationTestCase
{
    public function test_create_loader_with_invalid_operation() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Operation can be insert, update, or delete, invalid given.');

        (new DbalLoader($table, $this->postgresqlConnectionParams()))->withOperation('invalid');
    }

    public function test_create_loader_with_invalid_operation_from_connection() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Operation can be insert, update, or delete, invalid given.');

        DbalLoader::fromConnection(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            PostgreSQLInsertOptions::new(),
            'invalid'
        );
    }
}
