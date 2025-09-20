<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\CLI\Command\DatabaseTableSchemaCommand;
use Flow\CLI\Tests\Context\DatabaseContext;
use Flow\ETL\Tests\FlowTestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Flow\Filesystem\Tests\OperatingSystem;

final class DatabaseTableSchemaCommandTest extends FlowTestCase
{
    protected ?DatabaseContext $dbContext = null;

    use OperatingSystem;

    protected function setUp() : void
    {
        parent::setUp();

        if ($this->isWindows()) {
            self::markTestSkipped('DatabaseTableListCommand is not supported on Windows.');
        }

        $this->dbContext = new DatabaseContext();
        $this->dbContext->dropAllTables();
    }

    public function test_run_db_table_list() : void
    {
        $this->dbContext()->createTable(
            (new Table(
                'table_01',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                ],
            ))->setPrimaryKey(['id'])
        );

        $this->dbContext()->createTable(
            (new Table(
                'table_02',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('created_at', Type::getType(Types::DATETIME_IMMUTABLE), ['notnull' => true]),
                    new Column('tags', Type::getType(Types::JSON), ['notnull' => true, 'platformOptions' => ['jsonb' => true]]),
                ],
            ))->setPrimaryKey(['id'])
        );

        $tester = new CommandTester(new DatabaseTableSchemaCommand('db:table:schema'));

        $tester->execute([
            'input-db-table' => 'table_01',
            '--db-connection-file' => __DIR__ . '/Fixtures/connection.php',
            '--output-php' => true,
        ]);

        $tester->assertCommandIsSuccessful();


        // changeColumn was removed in doctrine/dbal 4.0
        // We are using it to perform a different assertion since prior to 4.0 all
        // columns were also getting precision set to 10 due to a bug that was executing precision set
        // even when precision value was null.
        if (!\method_exists(Table::class, 'changeColumn')) {
            self::assertSame(
                <<<'PHP'
\Flow\ETL\DSL\schema(
    \Flow\ETL\DSL\integer_schema("id", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata(["dbal_column_primary" => "table_01_pkey"])),
    \Flow\ETL\DSL\string_schema("name", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata(["dbal_column_length" => 255])),
    \Flow\ETL\DSL\string_schema("description", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata(["dbal_column_length" => 255])),
);

PHP,
                $tester->getDisplay()
            );
        } else {
            self::assertSame(
                <<<'PHP'
\Flow\ETL\DSL\schema(
    \Flow\ETL\DSL\integer_schema("id", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata(["dbal_column_precision" => 10, "dbal_column_primary" => "table_01_pkey"])),
    \Flow\ETL\DSL\string_schema("name", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata(["dbal_column_length" => 255, "dbal_column_precision" => 10])),
    \Flow\ETL\DSL\string_schema("description", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata(["dbal_column_length" => 255, "dbal_column_precision" => 10])),
);

PHP,
                $tester->getDisplay()
            );
        }
    }

    public function test_selecting_specific_columns_only() : void
    {
        // We don't need to test this edge case on doctrine/dbal below version 4.0 since the logic does not change.
        if (\method_exists(Table::class, 'changeColumn')) {
            $this->markTestSkipped('This test is not supported in doctrine/dbal 4.0');
        }

        $this->dbContext()->createTable(
            (new Table(
                'table_01',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                ],
            ))->setPrimaryKey(['id'])
        );

        $tester = new CommandTester(new DatabaseTableSchemaCommand('db:table:schema'));

        $tester->execute([
            'input-db-table' => 'table_01',
            '--db-connection-file' => __DIR__ . '/Fixtures/connection.php',
            '--db-column' => ['id', 'name'],
            '--output-php' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertSame(
            <<<'PHP'
\Flow\ETL\DSL\schema(
    \Flow\ETL\DSL\integer_schema("id", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata(["dbal_column_primary" => "table_01_pkey"])),
    \Flow\ETL\DSL\string_schema("name", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata(["dbal_column_length" => 255])),
);

PHP,
            $tester->getDisplay()
        );
    }

    public function test_selecting_not_existing_column() : void
    {
        // We don't need to test this edge case on doctrine/dbal below version 4.0 since the logic does not change.
        if (\method_exists(Table::class, 'changeColumn')) {
            $this->markTestSkipped('This test is not supported in doctrine/dbal 4.0');
        }

        $this->dbContext()->createTable(
            (new Table(
                'table_01',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                ],
            ))->setPrimaryKey(['id'])
        );

        $tester = new CommandTester(new DatabaseTableSchemaCommand('db:table:schema'));

        $tester->execute([
            'input-db-table' => 'table_01',
            '--db-connection-file' => __DIR__ . '/Fixtures/connection.php',
            '--db-column' => ['not_existing_one'],
            '--output-php' => true,
        ]);

        self::assertEquals(1, $tester->getStatusCode());
        self::assertStringContainsString(
            <<<'PHP'
[ERROR] Column "not_existing_one" not found in table "table_01".
PHP,
            $tester->getDisplay()
        );
    }

    protected function dbContext() : DatabaseContext
    {
        if (null === $this->dbContext) {
            $this->dbContext = new DatabaseContext();
        }

        return $this->dbContext;
    }
}
