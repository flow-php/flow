<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit;

use Doctrine\DBAL\{Connection, DriverManager};
use Doctrine\DBAL\Platforms\{AbstractPlatform, SQLitePlatform};
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\Doctrine\Bulk\{BulkData, SQLParametersStyle, TableDefinition};
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TableDefinitionTest extends TestCase
{
    /**
     * @return \Generator<string, array{array<string, mixed>, class-string<AbstractPlatform>}>
     */
    public static function provide_platform_types() : \Generator
    {
        yield 'sqlite' => [['driver' => 'sqlite3', 'memory' => true], SQLitePlatform::class];
    }

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_table_names() : \Generator
    {
        yield 'simple name' => ['users'];
        yield 'with underscore' => ['user_profiles'];
        yield 'with numbers' => ['table123'];
        yield 'mixed case' => ['UserProfiles'];
    }

    public function test_column_caching_works_correctly() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $column1 = $tableDefinition->dbalColumn('id');
        $column2 = $tableDefinition->dbalColumn('id');

        self::assertSame($column1->getName(), $column2->getName());
        self::assertSame(
            Type::getTypeRegistry()->lookupName($column1->getType()),
            Type::getTypeRegistry()->lookupName($column2->getType())
        );
    }

    public function test_column_retrieval_from_non_existent_table() : void
    {
        $connection = $this->createSQLiteConnection();

        $tableDefinition = new TableDefinition('non_existent_table', $connection);

        $this->expectException(\Exception::class);

        $tableDefinition->dbalColumn('any_column');
    }

    public function test_construct_creates_table_definition_with_name_and_connection() : void
    {
        $connection = $this->createSQLiteConnection();

        $tableDefinition = new TableDefinition('test_table', $connection);

        self::assertSame('test_table', $tableDefinition->name());
    }

    public function test_dbal_column_is_case_sensitive() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column with name ID, not found in table: test_table');

        $tableDefinition->dbalColumn('ID');
    }

    public function test_dbal_column_returns_column_with_correct_type() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $nameColumn = $tableDefinition->dbalColumn('name');

        self::assertSame('name', $nameColumn->getName());
        self::assertSame(Types::STRING, Type::getTypeRegistry()->lookupName($nameColumn->getType()));
    }

    public function test_dbal_column_returns_existing_column() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $column = $tableDefinition->dbalColumn('id');

        self::assertInstanceOf(Column::class, $column);
        self::assertSame('id', $column->getName());
    }

    public function test_dbal_column_throws_exception_for_empty_column_name() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column with name , not found in table: test_table');

        $tableDefinition->dbalColumn('');
    }

    public function test_dbal_column_throws_exception_for_nonexistent_column() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column with name nonexistent_column, not found in table: test_table');

        $tableDefinition->dbalColumn('nonexistent_column');
    }

    public function test_dbal_types_returns_correct_types_for_bulk_data() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
            ['id' => 2, 'name' => 'Jane'],
        ]);

        $types = $tableDefinition->dbalTypes($bulkData);

        self::assertSame([
            'id_0' => Types::INTEGER,
            'id_1' => Types::INTEGER,
            'name_0' => Types::STRING,
            'name_1' => Types::STRING,
        ], $types);
    }

    public function test_dbal_types_returns_correct_types_for_single_row() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
        ]);

        $types = $tableDefinition->dbalTypes($bulkData);

        self::assertSame([
            'id_0' => Types::INTEGER,
            'name_0' => Types::STRING,
        ], $types);
    }

    public function test_dbal_types_throws_exception_for_invalid_column() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'invalid_column' => 'value'],
        ]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column with name invalid_column, not found in table: test_table');

        $tableDefinition->dbalTypes($bulkData);
    }

    public function test_dbal_types_with_different_column_types() : void
    {
        $connection = $this->createConnectionWithComplexTable('complex_table');
        $tableDefinition = new TableDefinition('complex_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John', 'age' => 30, 'active' => true, 'created_at' => '2023-01-01 12:00:00'],
        ]);

        $types = $tableDefinition->dbalTypes($bulkData);

        self::assertSame([
            'id_0' => Types::INTEGER,
            'name_0' => Types::STRING,
            'age_0' => Types::INTEGER,
            'active_0' => Types::BOOLEAN,
            'created_at_0' => Types::DATETIME_MUTABLE,
        ], $types);
    }

    public function test_dbal_types_with_empty_bulk_data_throws_exception() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Bulk data cannot be empty');

        new BulkData([]);
    }

    public function test_different_table_names_work_correctly() : void
    {
        $connection = $this->createSQLiteConnection();
        $this->createTestTable($connection, 'users');
        $this->createTestTable($connection, 'products');

        $usersTable = new TableDefinition('users', $connection);
        $productsTable = new TableDefinition('products', $connection);

        self::assertSame('users', $usersTable->name());
        self::assertSame('products', $productsTable->name());

        self::assertInstanceOf(Column::class, $usersTable->dbalColumn('id'));
        self::assertInstanceOf(Column::class, $productsTable->dbalColumn('id'));
    }

    public function test_name_returns_table_name() : void
    {
        $connection = $this->createSQLiteConnection();

        $tableDefinition = new TableDefinition('users', $connection);

        self::assertSame('users', $tableDefinition->name());
    }

    public function test_name_returns_table_name_with_special_characters() : void
    {
        $connection = $this->createSQLiteConnection();

        $tableDefinition = new TableDefinition('test_table_123', $connection);

        self::assertSame('test_table_123', $tableDefinition->name());
    }

    public function test_platform_returns_connection_database_platform() : void
    {
        $connection = $this->createSQLiteConnection();

        $tableDefinition = new TableDefinition('test_table', $connection);

        self::assertInstanceOf(SQLitePlatform::class, $tableDefinition->platform());
    }

    /**
     * @param array<string, mixed> $connectionParams
     * @param class-string<AbstractPlatform> $expectedPlatformClass
     */
    #[DataProvider('provide_platform_types')]
    public function test_platform_returns_correct_platform_type(array $connectionParams, string $expectedPlatformClass) : void
    {
        $connection = DriverManager::getConnection($connectionParams);

        $tableDefinition = new TableDefinition('test_table', $connection);

        /** @var class-string<AbstractPlatform> $expectedPlatformClass */
        self::assertInstanceOf($expectedPlatformClass, $tableDefinition->platform());
    }

    #[DataProvider('provide_table_names')]
    public function test_table_name_handling(string $tableName) : void
    {
        $connection = $this->createConnectionWithTable($tableName);
        $tableDefinition = new TableDefinition($tableName, $connection);

        self::assertSame($tableName, $tableDefinition->name());
        self::assertInstanceOf(Column::class, $tableDefinition->dbalColumn('id'));
    }

    public function test_to_sql_casted_placeholders_generates_correct_sql() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
        ], [], SQLParametersStyle::NAMED);

        $sql = $bulkData->toSqlCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(:id_0 as INTEGER)', $sql);
        self::assertStringContainsString('CAST(:name_0 as VARCHAR', $sql);
        self::assertStringContainsString('(', $sql);
        self::assertStringContainsString(')', $sql);
    }

    public function test_to_sql_casted_placeholders_throws_exception_for_invalid_column() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'invalid_column' => 'value'],
        ], [], SQLParametersStyle::NAMED);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column with name invalid_column, not found in table: test_table');

        $bulkData->toSqlCastedPlaceholders($tableDefinition);
    }

    public function test_to_sql_casted_placeholders_with_different_platforms() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
        ], [], SQLParametersStyle::NAMED);

        $sql = $bulkData->toSqlCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(:id_0 as ', $sql);
        self::assertStringContainsString('CAST(:name_0 as ', $sql);
    }

    public function test_to_sql_casted_placeholders_with_empty_rows() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);
        $platform = $tableDefinition->platform();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Bulk data cannot be empty');

        new BulkData([]);
    }

    public function test_to_sql_casted_placeholders_with_multiple_rows() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);
        $platform = $tableDefinition->platform();

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
            ['id' => 2, 'name' => 'Jane'],
        ], [], SQLParametersStyle::NAMED);

        $sql = $bulkData->toSqlCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(:id_0 as INTEGER)', $sql);
        self::assertStringContainsString('CAST(:name_0 as VARCHAR', $sql);
        self::assertStringContainsString('CAST(:id_1 as INTEGER)', $sql);
        self::assertStringContainsString('CAST(:name_1 as VARCHAR', $sql);
        self::assertStringContainsString(',', $sql);

        // The SQL should contain 2 sets of parentheses, one for each row
        $rowCount = substr_count($sql, '(CAST');
        self::assertSame(2, $rowCount);
    }

    private function createComplexTestTable(Connection $connection, string $tableName) : void
    {
        $table = new Table(
            $tableName,
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('age', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
                new Column('created_at', Type::getType(Types::DATETIME_MUTABLE), ['notnull' => true]),
            ]
        );

        $connection->createSchemaManager()->createTable($table);
    }

    private function createConnectionWithComplexTable(string $tableName) : Connection
    {
        $connection = $this->createSQLiteConnection();
        $this->createComplexTestTable($connection, $tableName);

        return $connection;
    }

    private function createConnectionWithTable(string $tableName) : Connection
    {
        $connection = $this->createSQLiteConnection();
        $this->createTestTable($connection, $tableName);

        return $connection;
    }

    private function createSQLiteConnection() : Connection
    {
        return DriverManager::getConnection([
            'driver' => 'sqlite3',
            'memory' => true,
        ]);
    }

    private function createTestTable(Connection $connection, string $tableName) : void
    {
        $table = new Table(
            $tableName,
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ]
        );

        $connection->createSchemaManager()->createTable($table);
    }
}
