<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\IO\Loader;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\IntegerType as DoctrineIntegerType;
use Doctrine\DBAL\Types\TextType;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\Doctrine\Bulk\Dialect\PostgreSQLInsertOptions;
use Flow\ETL\Adapter\Doctrine\IO\Loader\DbalLoader;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Adapter\Doctrine\Values\Schema\TypesMap;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;

final class DbalLoaderTest extends IntegrationTestCase
{
    public function test_create_loader_with_invalid_operation(): void
    {
        $this->pgsqlDatabaseContext->createTable(
            (new Table($table = 'flow_doctrine_bulk_test', [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ]))
                ->edit()
                ->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create())
                ->create(),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Operation can be insert, update, or delete, invalid given.');

        (new DbalLoader($table, $this->postgresqlConnectionParams()))->withOperation('invalid');
    }

    public function test_create_loader_with_invalid_operation_from_connection(): void
    {
        $this->pgsqlDatabaseContext->createTable(
            (new Table($table = 'flow_doctrine_bulk_test', [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ]))
                ->edit()
                ->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create())
                ->create(),
        );

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
        $this->pgsqlDatabaseContext->createTable(
            (new Table($table = 'flow_doctrine_bulk_test', [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::TEXT), ['notnull' => false]),
            ]))
                ->edit()
                ->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create())
                ->create(),
        );

        data_frame()
            ->read(from_array([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
            ]))
            ->write((new DbalLoader($table, $this->postgresqlConnectionParams()))->withTypesMap(new TypesMap([
                StringType::class => TextType::class,
                IntegerType::class => DoctrineIntegerType::class,
            ])))
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

    public function test_describe_round_trips(): void
    {
        $description = (new DbalLoader('flow_items', $this->sqliteConnectionParams()))
            ->withOperation('update')
            ->describe();

        static::assertNotNull($description);
        static::assertTrue($description->sensitive());
    }

    public function test_operation_options_do_not_describe(): void
    {
        static::assertNull(
            (new DbalLoader('flow_items', $this->sqliteConnectionParams()))
                ->withOperationOptions(PostgreSQLInsertOptions::new())
                ->describe(),
        );
        static::assertNull(
            (new DbalLoader('flow_items', $this->sqliteConnectionParams()))
                ->withTypesMap(new TypesMap([]))
                ->describe(),
        );
    }
}
