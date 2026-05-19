<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\IntegerType as DoctrineIntegerType;
use Doctrine\DBAL\Types\TextType;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\Doctrine\Bulk\Dialect\PostgreSQLInsertOptions;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Adapter\Doctrine\TypesMap;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;

final class DbalLoaderTest extends IntegrationTestCase
{
    public function test_create_loader_with_invalid_operation(): void
    {
        $this->pgsqlDatabaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        ]))->setPrimaryKey(['id']));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Operation can be insert, update, or delete, invalid given.');

        (new DbalLoader($table, $this->postgresqlConnectionParams()))->withOperation('invalid');
    }

    public function test_create_loader_with_invalid_operation_from_connection(): void
    {
        $this->pgsqlDatabaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        ]))->setPrimaryKey(['id']));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Operation can be insert, update, or delete, invalid given.');

        DbalLoader::fromConnection(
            $this->pgsqlDatabaseContext->connection(),
            $table,
            PostgreSQLInsertOptions::new(),
            'invalid',
        );
    }

    public function test_loader_with_custom_types_map(): void
    {
        $this->pgsqlDatabaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::TEXT), ['notnull' => false]),
        ]))->setPrimaryKey(['id']));

        $customTypesMap = new TypesMap([
            StringType::class => TextType::class,
            IntegerType::class => DoctrineIntegerType::class,
        ]);

        $loader = (new DbalLoader($table, $this->postgresqlConnectionParams()))->withTypesMap($customTypesMap);

        data_frame()
            ->read(from_array([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
            ]))
            ->load($loader)
            ->run();

        static::assertEquals(2, $this->pgsqlDatabaseContext->tableCount($table));
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
            ],
            $this->pgsqlDatabaseContext->selectAll($table),
        );
    }
}
