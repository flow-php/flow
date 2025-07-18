<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit;

use Doctrine\DBAL\{Connection, DriverManager};
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\Doctrine\Bulk\{TableDefinition, TableDefinitions};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TableDefinitionsTest extends TestCase
{
    public static function provide_table_names() : \Generator
    {
        yield 'simple name' => ['users'];
        yield 'with underscore' => ['user_profiles'];
        yield 'with numbers' => ['table123'];
        yield 'mixed case' => ['UserProfiles'];
        yield 'single char' => ['a'];
        yield 'long name' => ['very_long_table_name_with_many_characters'];
    }

    /**
     * This test documents a potential issue in the caching logic.
     * The current implementation searches through all cached tables by name,
     * but the key in the array is also the name. This makes the search inefficient
     * and potentially problematic if the same name is used with different connections.
     */
    public function test_caching_logic_inefficiency() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection1 = $this->createConnectionWithTable('users');
        $connection2 = $this->createConnectionWithTable('users');

        $definition1 = $tableDefinitions->get('users', $connection1);
        $definition2 = $tableDefinitions->get('users', $connection2);

        // This test documents that the current implementation will return the same
        // TableDefinition instance even with different connections, which might not
        // be the intended behavior since the TableDefinition holds a reference to
        // the specific connection.
        self::assertSame($definition1, $definition2);

        // The cached definition will still use the first connection
        self::assertSame($connection1->getDatabasePlatform(), $definition1->platform());
    }

    public function test_construct_creates_empty_table_definitions() : void
    {
        $tableDefinitions = new TableDefinitions();

        // We can't directly test the private array, so we test the behavior
        // First get() should create a new TableDefinition
        $connection = $this->createConnectionWithTable('test_table');
        $tableDefinition = $tableDefinitions->get('test_table', $connection);

        self::assertInstanceOf(TableDefinition::class, $tableDefinition);
        self::assertSame('test_table', $tableDefinition->name());
    }

    public function test_get_caches_multiple_table_definitions() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createSQLiteConnection();
        $this->createTestTable($connection, 'users');
        $this->createTestTable($connection, 'products');
        $this->createTestTable($connection, 'orders');

        $usersDefinition1 = $tableDefinitions->get('users', $connection);
        $productsDefinition1 = $tableDefinitions->get('products', $connection);
        $ordersDefinition1 = $tableDefinitions->get('orders', $connection);

        // Get the same definitions again
        $usersDefinition2 = $tableDefinitions->get('users', $connection);
        $productsDefinition2 = $tableDefinitions->get('products', $connection);
        $ordersDefinition2 = $tableDefinitions->get('orders', $connection);

        self::assertSame($usersDefinition1, $usersDefinition2);
        self::assertSame($productsDefinition1, $productsDefinition2);
        self::assertSame($ordersDefinition1, $ordersDefinition2);

        // Verify they are different instances
        self::assertNotSame($usersDefinition1, $productsDefinition1);
        self::assertNotSame($usersDefinition1, $ordersDefinition1);
        self::assertNotSame($productsDefinition1, $ordersDefinition1);
    }

    public function test_get_creates_different_table_definitions_for_different_names() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createSQLiteConnection();
        $this->createTestTable($connection, 'users');
        $this->createTestTable($connection, 'products');

        $usersDefinition = $tableDefinitions->get('users', $connection);
        $productsDefinition = $tableDefinitions->get('products', $connection);

        self::assertNotSame($usersDefinition, $productsDefinition);
        self::assertSame('users', $usersDefinition->name());
        self::assertSame('products', $productsDefinition->name());
    }

    public function test_get_creates_new_table_definition_for_first_time() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createConnectionWithTable('users');

        $tableDefinition = $tableDefinitions->get('users', $connection);

        self::assertInstanceOf(TableDefinition::class, $tableDefinition);
        self::assertSame('users', $tableDefinition->name());
    }

    public function test_get_performance_with_repeated_calls() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createConnectionWithTable('users');

        $tableDefinition1 = $tableDefinitions->get('users', $connection);

        // Multiple calls should return the same instance (cached)
        for ($i = 0; $i < 10; $i++) {
            $tableDefinition = $tableDefinitions->get('users', $connection);
            self::assertSame($tableDefinition1, $tableDefinition);
        }
    }

    public function test_get_preserves_connection_in_table_definition() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createConnectionWithTable('users');

        $tableDefinition = $tableDefinitions->get('users', $connection);

        // Verify the connection is preserved by testing platform access
        $platform = $tableDefinition->platform();
        self::assertSame($connection->getDatabasePlatform(), $platform);
    }

    public function test_get_returns_cached_table_definition_for_same_name() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createConnectionWithTable('users');

        $tableDefinition1 = $tableDefinitions->get('users', $connection);
        $tableDefinition2 = $tableDefinitions->get('users', $connection);

        self::assertSame($tableDefinition1, $tableDefinition2);
    }

    public function test_get_returns_working_table_definition() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createConnectionWithTable('users');

        $tableDefinition = $tableDefinitions->get('users', $connection);

        // Verify the returned TableDefinition is functional
        self::assertInstanceOf(TableDefinition::class, $tableDefinition);
        self::assertSame('users', $tableDefinition->name());

        // Test that it can access database metadata
        $column = $tableDefinition->dbalColumn('id');
        self::assertInstanceOf(Column::class, $column);
        self::assertSame('id', $column->getName());
    }

    public function test_get_with_different_connections_for_same_name() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection1 = $this->createConnectionWithTable('users');
        $connection2 = $this->createConnectionWithTable('users');

        $tableDefinition1 = $tableDefinitions->get('users', $connection1);
        $tableDefinition2 = $tableDefinitions->get('users', $connection2);

        // NOTE: This reveals a potential issue - the caching logic only checks by name,
        // not by connection. This means the same TableDefinition instance will be reused
        // even with different connections, which might not be the intended behavior.
        self::assertSame($tableDefinition1, $tableDefinition2);
    }

    public function test_get_with_different_table_names_maintains_separate_cache() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createSQLiteConnection();
        $this->createTestTable($connection, 'table_a');
        $this->createTestTable($connection, 'table_b');

        $definitionA1 = $tableDefinitions->get('table_a', $connection);
        $definitionB1 = $tableDefinitions->get('table_b', $connection);
        $definitionA2 = $tableDefinitions->get('table_a', $connection);
        $definitionB2 = $tableDefinitions->get('table_b', $connection);

        // Same names should return same instances
        self::assertSame($definitionA1, $definitionA2);
        self::assertSame($definitionB1, $definitionB2);

        // Different names should return different instances
        self::assertNotSame($definitionA1, $definitionB1);
    }

    public function test_get_with_empty_table_name() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createSQLiteConnection();

        // This will create a TableDefinition with empty name, which may cause issues
        // when trying to query the actual database table
        $tableDefinition = $tableDefinitions->get('', $connection);

        self::assertInstanceOf(TableDefinition::class, $tableDefinition);
        self::assertSame('', $tableDefinition->name());
    }

    public function test_get_with_non_existent_table() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createSQLiteConnection();

        // This should create a TableDefinition even for non-existent tables
        // The error will occur when trying to use the TableDefinition
        $tableDefinition = $tableDefinitions->get('non_existent_table', $connection);

        self::assertInstanceOf(TableDefinition::class, $tableDefinition);
        self::assertSame('non_existent_table', $tableDefinition->name());
    }

    public function test_get_with_special_character_table_names() : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createSQLiteConnection();

        // Test with special characters that don't require actual database tables
        $specialNames = ['user-profiles', 'table.name', 'table name', 'table@name'];

        foreach ($specialNames as $name) {
            $tableDefinition = $tableDefinitions->get($name, $connection);

            self::assertInstanceOf(TableDefinition::class, $tableDefinition);
            self::assertSame($name, $tableDefinition->name());
        }
    }

    #[DataProvider('provide_table_names')]
    public function test_get_with_various_table_names(string $tableName) : void
    {
        $tableDefinitions = new TableDefinitions();
        $connection = $this->createConnectionWithTable($tableName);

        $tableDefinition = $tableDefinitions->get($tableName, $connection);

        self::assertInstanceOf(TableDefinition::class, $tableDefinition);
        self::assertSame($tableName, $tableDefinition->name());
    }

    public function test_multiple_table_definitions_instances_work_independently() : void
    {
        $tableDefinitions1 = new TableDefinitions();
        $tableDefinitions2 = new TableDefinitions();
        $connection = $this->createConnectionWithTable('users');

        $definition1 = $tableDefinitions1->get('users', $connection);
        $definition2 = $tableDefinitions2->get('users', $connection);

        // Different TableDefinitions instances should create separate caches
        self::assertNotSame($definition1, $definition2);
        self::assertSame('users', $definition1->name());
        self::assertSame('users', $definition2->name());
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
