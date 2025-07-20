<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit;

use Doctrine\DBAL\{Connection, DriverManager};
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\Doctrine\Bulk\{BulkData, Columns, SQLParametersStyle, TableDefinition};
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class BulkDataTest extends TestCase
{
    public static function provide_parameter_style_combinations() : \Generator
    {
        yield 'named parameters' => [SQLParametersStyle::NAMED, ':'];
        yield 'positional parameters' => [SQLParametersStyle::POSITIONAL, '?'];
    }

    public static function provide_single_row_data() : \Generator
    {
        yield 'integer column' => [['id' => 42], 'INTEGER'];
        yield 'string column' => [['name' => 'test'], 'VARCHAR'];
        yield 'boolean column' => [['active' => true], 'BOOLEAN'];
    }

    public function test_parameter_style_affects_all_sql_generation_methods() : void
    {
        $connection = $this->createConnectionWithTable('style_test');
        $tableDefinition = new TableDefinition('style_test', $connection);

        $namedBulkData = new BulkData([
            ['id' => 1, 'name' => 'Named'],
        ], [], SQLParametersStyle::NAMED);

        $positionalBulkData = new BulkData([
            ['id' => 1, 'name' => 'Positional'],
        ], [], SQLParametersStyle::POSITIONAL);

        $namedPlaceholders = $namedBulkData->toSqlPlaceholders();
        $positionalPlaceholders = $positionalBulkData->toSqlPlaceholders();

        $namedCasted = $namedBulkData->toSqlCastedPlaceholders($tableDefinition);
        $positionalCasted = $positionalBulkData->toSqlCastedPlaceholders($tableDefinition);

        self::assertStringContainsString(':', $namedPlaceholders);
        self::assertStringNotContainsString(':', $positionalPlaceholders);

        self::assertStringContainsString(':id_0', $namedCasted);
        self::assertStringNotContainsString(':', $positionalCasted);
        self::assertStringContainsString('?', $positionalCasted);
    }

    public function test_prevents_creating_bulk_data_for_different_rows() : void
    {
        $this->expectExceptionMessage('Each row must be have the same keys in the same order');

        new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            [
                'title' => 'Title One',
                'date' => 'today',
                'quantity' => 101,
                'description' => 'Description One',
            ],
        ]);
    }

    public function test_prevents_creating_bulk_data_from_invalid_rows() : void
    {
        $this->expectExceptionMessage('Each row must be an array');

        new BulkData([1, 2, 3]);
    }

    public function test_prevents_creating_bulk_data_from_invalid_rows_when_first_row_is_an_array() : void
    {
        $this->expectExceptionMessage('Each row must be an array');

        new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            'not-an-array',
        ]);
    }

    public function test_prevents_creating_empty_bulk_data() : void
    {
        $this->expectExceptionMessage('Bulk data cannot be empty');

        new BulkData([]);
    }

    public function test_returns_columns() : void
    {
        $bulkData = new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            [
                'date' => 'today',
                'title' => 'Title Two',
                'description' => 'Description Two',
                'quantity' => 102,
            ],
        ]);

        self::assertEquals(
            new Columns('date', 'title', 'description', 'quantity'),
            $bulkData->columns()
        );
    }

    public function test_returns_rows_with_numeric_indexes_even_when_provided_no_sorted() : void
    {
        $bulkData = new BulkData([
            5 => [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            10 => [
                'date' => 'today',
                'title' => 'Title Two',
                'description' => 'Description Two',
                'quantity' => 102,
            ],
        ]);

        self::assertEquals(
            [
                0 => [
                    'date' => 'today',
                    'title' => 'Title One',
                    'description' => 'Description One',
                    'quantity' => 101,
                ],
                1 => [
                    'date' => 'today',
                    'title' => 'Title Two',
                    'description' => 'Description Two',
                    'quantity' => 102,
                ],
            ],
            $bulkData->rows()
        );
    }

    public function test_returns_sql_rows() : void
    {
        $bulkData = new BulkData([
            5 => [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            10 => [
                'date' => 'today',
                'title' => 'Title Two',
                'description' => 'Description Two',
                'quantity' => 102,
            ],
        ]);

        self::assertEquals(
            [
                0 => [
                    'date_0' => 'today',
                    'title_0' => 'Title One',
                    'description_0' => 'Description One',
                    'quantity_0' => 101,
                ],
                1 => [
                    'date_1' => 'today',
                    'title_1' => 'Title Two',
                    'description_1' => 'Description Two',
                    'quantity_1' => 102,
                ],
            ],
            $bulkData->sqlRows()
        );
    }

    public function test_to_sql_casted_placeholders_defaults_to_positional_when_no_style_specified() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
        ]);

        $result = $bulkData->toSqlCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(? as INTEGER)', $result);
        self::assertStringContainsString('CAST(? as VARCHAR', $result);
        self::assertStringNotContainsString(':id_', $result);
    }

    public function test_to_sql_casted_placeholders_falls_back_to_database_when_no_custom_type() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        // Provide custom type for only one column
        $customTypes = [
            'id' => Type::getType(Types::INTEGER),
            // 'name' not provided - should fall back to database lookup
        ];

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'Test'],
        ], $customTypes, SQLParametersStyle::POSITIONAL);

        $result = $bulkData->toSqlPositionalCastedPlaceholders($tableDefinition);

        // id uses custom type, name uses database column type
        self::assertStringContainsString('CAST(? as INTEGER)', $result); // Custom type
        self::assertStringContainsString('CAST(? as VARCHAR', $result); // Database column type with length
    }

    public function test_to_sql_casted_placeholders_handles_special_characters_in_column_names() : void
    {
        $connection = $this->createSQLiteConnection();

        $table = new Table(
            'special_table',
            [
                new Column('user_id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('first_name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 100]),
            ]
        );
        $connection->createSchemaManager()->createTable($table);

        $tableDefinition = new TableDefinition('special_table', $connection);

        $bulkData = new BulkData([
            ['user_id' => 1, 'first_name' => 'John'],
        ], [], SQLParametersStyle::NAMED);

        $result = $bulkData->toSqlNamedCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(:user_id_0 as INTEGER)', $result);
        self::assertStringContainsString('CAST(:first_name_0 as VARCHAR', $result);
    }

    public function test_to_sql_casted_placeholders_throws_exception_for_invalid_column() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'invalid_column' => 'value'],
        ]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column with name invalid_column, not found in table: test_table');

        $bulkData->toSqlCastedPlaceholders($tableDefinition);
    }

    public function test_to_sql_casted_placeholders_with_complex_table_structure() : void
    {
        $connection = $this->createConnectionWithComplexTable('complex_table');
        $tableDefinition = new TableDefinition('complex_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John', 'age' => 25, 'active' => true],
        ], [], SQLParametersStyle::NAMED);

        $result = $bulkData->toSqlCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(:id_0 as INTEGER)', $result);
        self::assertStringContainsString('CAST(:name_0 as VARCHAR', $result);
        self::assertStringContainsString('CAST(:age_0 as INTEGER)', $result);
        self::assertStringContainsString('CAST(:active_0 as BOOLEAN)', $result);
    }

    public function test_to_sql_casted_placeholders_with_custom_types() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $customTypes = [
            'id' => Type::getType(Types::INTEGER),
            'name' => Type::getType(Types::STRING),
        ];

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'CustomTypes'],
        ], $customTypes, SQLParametersStyle::NAMED);

        $result = $bulkData->toSqlNamedCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(:id_0 as INTEGER)', $result);
        self::assertStringContainsString('CAST(:name_0 as VARCHAR', $result);
    }

    public function test_to_sql_casted_placeholders_with_large_dataset() : void
    {
        $connection = $this->createConnectionWithTable('large_table');
        $tableDefinition = new TableDefinition('large_table', $connection);

        $rows = [];

        for ($i = 1; $i <= 100; $i++) {
            $rows[] = ['id' => $i, 'name' => "Name{$i}"];
        }

        $bulkData = new BulkData($rows, [], SQLParametersStyle::POSITIONAL);

        $result = $bulkData->toSqlPositionalCastedPlaceholders($tableDefinition);

        $castCount = substr_count($result, 'CAST(? as INTEGER)');
        self::assertSame(100, $castCount);

        $nameCount = substr_count($result, 'CAST(? as VARCHAR');
        self::assertSame(100, $nameCount);

        $parenthesesCount = substr_count($result, '(CAST');
        self::assertSame(100, $parenthesesCount);
    }

    public function test_to_sql_casted_placeholders_with_named_parameters() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
            ['id' => 2, 'name' => 'Jane'],
        ], [], SQLParametersStyle::NAMED);

        $result = $bulkData->toSqlCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(:id_0 as INTEGER)', $result);
        self::assertStringContainsString('CAST(:name_0 as VARCHAR', $result);
        self::assertStringContainsString('CAST(:id_1 as INTEGER)', $result);
        self::assertStringContainsString('CAST(:name_1 as VARCHAR', $result);
        self::assertStringContainsString(',', $result);
    }

    public function test_to_sql_casted_placeholders_with_positional_parameters() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
            ['id' => 2, 'name' => 'Jane'],
        ], [], SQLParametersStyle::POSITIONAL);

        $result = $bulkData->toSqlCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(? as INTEGER)', $result);
        self::assertStringContainsString('CAST(? as VARCHAR', $result);
        self::assertStringContainsString(',', $result);

        self::assertStringNotContainsString(':id_', $result);
        self::assertStringNotContainsString(':name_', $result);
    }

    #[DataProvider('provide_single_row_data')]
    public function test_to_sql_casted_placeholders_with_single_column_data(array $rowData, string $expectedColumnType) : void
    {
        $connection = $this->createConnectionWithComplexTable('single_col_table');
        $tableDefinition = new TableDefinition('single_col_table', $connection);

        $bulkData = new BulkData([$rowData], [], SQLParametersStyle::POSITIONAL);

        $result = $bulkData->toSqlPositionalCastedPlaceholders($tableDefinition);

        self::assertStringContainsString("CAST(? as {$expectedColumnType}", $result);
    }

    public function test_to_sql_casted_placeholders_with_single_row() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'SingleRow'],
        ], [], SQLParametersStyle::NAMED);

        $result = $bulkData->toSqlCastedPlaceholders($tableDefinition);

        self::assertStringNotContainsString('_1', $result);
    }

    public function test_to_sql_named_casted_placeholders_throws_exception_for_invalid_column() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'unknown_field' => 'value'],
        ]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column with name unknown_field, not found in table: test_table');

        $bulkData->toSqlNamedCastedPlaceholders($tableDefinition);
    }

    public function test_to_sql_named_casted_placeholders_with_multiple_rows() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
            ['id' => 2, 'name' => 'Jane'],
            ['id' => 3, 'name' => 'Bob'],
        ]);

        $result = $bulkData->toSqlNamedCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(:id_0 as INTEGER)', $result);
        self::assertStringContainsString('CAST(:name_0 as VARCHAR', $result);
        self::assertStringContainsString('CAST(:id_1 as INTEGER)', $result);
        self::assertStringContainsString('CAST(:name_1 as VARCHAR', $result);
        self::assertStringContainsString('CAST(:id_2 as INTEGER)', $result);
        self::assertStringContainsString('CAST(:name_2 as VARCHAR', $result);

        $parenthesesCount = substr_count($result, '(CAST');
        self::assertSame(3, $parenthesesCount);
    }

    #[DataProvider('provide_parameter_style_combinations')]
    public function test_to_sql_parameters_dispatcher_method(SQLParametersStyle $style, string $expectedKeyPattern) : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
        ], [], $style);

        $result = $bulkData->toSqlParameters($tableDefinition);

        if ($style === SQLParametersStyle::NAMED) {
            self::assertArrayHasKey('id_0', $result);
            self::assertArrayHasKey('name_0', $result);
            self::assertSame(1, $result['id_0']);
            self::assertSame('John', $result['name_0']);
        } else {
            self::assertIsArray($result);
            self::assertCount(2, $result);
            self::assertContains(1, $result);
            self::assertContains('John', $result);
        }
    }

    public function test_to_sql_parameters_falls_back_to_table_column_types() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => '456', 'name' => 'TableTypes'],
        ], [], SQLParametersStyle::POSITIONAL);

        $result = $bulkData->toSqlParameters($tableDefinition);

        self::assertCount(2, $result);
        self::assertSame('456', $result[0]);
        self::assertSame('TableTypes', $result[1]);
    }

    public function test_to_sql_parameters_with_custom_types_conversion() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        // Custom type that converts values
        $customTypes = [
            'id' => Type::getType(Types::INTEGER),
        ];

        $bulkData = new BulkData([
            ['id' => '123', 'name' => 'TypeConversion'],
        ], $customTypes, SQLParametersStyle::NAMED);

        $result = $bulkData->toSqlParameters($tableDefinition);

        self::assertArrayHasKey('id_0', $result);
        self::assertArrayHasKey('name_0', $result);
    }

    #[DataProvider('provide_parameter_style_combinations')]
    public function test_to_sql_placeholders_dispatcher_method(SQLParametersStyle $style, string $expectedPattern) : void
    {
        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
            ['id' => 2, 'name' => 'Jane'],
        ], [], $style);

        $result = $bulkData->toSqlPlaceholders();

        self::assertStringContainsString($expectedPattern, $result);

        if ($style === SQLParametersStyle::NAMED) {
            self::assertStringContainsString(':id_0', $result);
            self::assertStringContainsString(':name_0', $result);
            self::assertStringContainsString(':id_1', $result);
            self::assertStringContainsString(':name_1', $result);
        } else {
            self::assertStringContainsString('?', $result);
            self::assertStringNotContainsString(':', $result);
        }
    }

    public function test_to_sql_positional_casted_placeholders_throws_exception_for_invalid_column() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'missing_column' => 'value'],
        ]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column with name missing_column, not found in table: test_table');

        $bulkData->toSqlPositionalCastedPlaceholders($tableDefinition);
    }

    public function test_to_sql_positional_casted_placeholders_with_multiple_rows() : void
    {
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = new TableDefinition('test_table', $connection);

        $bulkData = new BulkData([
            ['id' => 1, 'name' => 'John'],
            ['id' => 2, 'name' => 'Jane'],
        ]);

        $result = $bulkData->toSqlPositionalCastedPlaceholders($tableDefinition);

        self::assertStringContainsString('CAST(? as INTEGER)', $result);
        self::assertStringContainsString('CAST(? as VARCHAR', $result);
        self::assertStringContainsString(',', $result);

        // Should have 2 sets of parentheses
        $parenthesesCount = substr_count($result, '(CAST');
        self::assertSame(2, $parenthesesCount);

        // Should not contain any named parameter markers
        self::assertStringNotContainsString(':', $result);
    }

    public function test_transforms_data_to_sql_positional_placeholders() : void
    {
        $bulkData = new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            [
                'date' => 'today',
                'title' => 'Title Two',
                'description' => 'Description Two',
                'quantity' => 102,
            ],
        ]);

        self::assertEquals(
            '(?,?,?,?),(?,?,?,?)',
            $bulkData->toSqlPositionalPlaceholders()
        );
    }

    public function test_transforms_data_to_sql_values_placeholders() : void
    {
        $bulkData = new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'description' => 'Description One',
                'quantity' => 101,
            ],
            [
                'date' => 'today',
                'title' => 'Title Two',
                'description' => 'Description Two',
                'quantity' => 102,
            ],
        ]);

        self::assertEquals(
            '(:date_0,:title_0,:description_0,:quantity_0),(:date_1,:title_1,:description_1,:quantity_1)',
            $bulkData->toSqlNamedPlaceholders()
        );
    }

    public function test_uses_named_parameters_by_default_when_explicitly_set() : void
    {
        $bulkData = new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'quantity' => 101,
            ],
        ], [], SQLParametersStyle::NAMED);

        self::assertEquals(
            '(:date_0,:title_0,:quantity_0)',
            $bulkData->toSqlPlaceholders()
        );
    }

    public function test_uses_positional_parameters_by_default() : void
    {
        $bulkData = new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'quantity' => 101,
            ],
        ]);

        self::assertEquals(
            '(?,?,?)',
            $bulkData->toSqlPlaceholders()
        );
    }

    public function test_uses_positional_parameters_when_explicitly_set() : void
    {
        $bulkData = new BulkData([
            [
                'date' => 'today',
                'title' => 'Title One',
                'quantity' => 101,
            ],
        ], [], SQLParametersStyle::POSITIONAL);

        self::assertEquals(
            '(?,?,?)',
            $bulkData->toSqlPlaceholders()
        );
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
