<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Dialects;

use function Flow\ETL\Adapter\Doctrine\{to_dbal_table_delete, to_dbal_table_insert, to_dbal_table_update};
use function Flow\ETL\DSL\{data_frame, from_array, ref};
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\Doctrine\Bulk\Dialect\{MySQLInsertOptions, MySQLUpdateOptions};
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;

final class MySQLDbalLoaderTest extends IntegrationTestCase
{
    public function test_delete_non_existent_rows() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        (data_frame())
            ->read(from_array([
                ['id' => 1, 'name' => 'Name One'],
                ['id' => 2, 'name' => 'Name Two'],
            ]))
            ->load(to_dbal_table_insert($this->mysqlConnectionParams(), $table))
            ->run();

        self::assertEquals(2, $this->mysqlDatabaseContext->tableCount($table));

        (data_frame())
            ->read(from_array([
                ['id' => 3],
                ['id' => 4],
            ]))
            ->load(to_dbal_table_delete($this->mysqlConnectionParams(), $table))
            ->run();

        self::assertEquals(2, $this->mysqlDatabaseContext->tableCount($table));
        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One'],
                ['id' => 2, 'name' => 'Name Two'],
            ],
            $this->mysqlDatabaseContext->selectAll($table)
        );
    }

    public function test_delete_rows_using_existing_connection() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        (data_frame())
            ->read(from_array([
                ['id' => 1, 'name' => 'Name One'],
                ['id' => 2, 'name' => 'Name Two'],
                ['id' => 3, 'name' => 'Name Three'],
            ]))
            ->load(to_dbal_table_insert($this->mysqlDatabaseContext->connection(), $table))
            ->run();

        self::assertEquals(3, $this->mysqlDatabaseContext->tableCount($table));

        (data_frame())
            ->read(from_array([
                ['id' => 2],
            ]))
            ->load(to_dbal_table_delete($this->mysqlDatabaseContext->connection(), $table))
            ->run();

        self::assertEquals(2, $this->mysqlDatabaseContext->tableCount($table));
        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One'],
                ['id' => 3, 'name' => 'Name Three'],
            ],
            $this->mysqlDatabaseContext->selectAll($table)
        );
    }

    public function test_delete_with_composite_keys() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('group_id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id', 'group_id']));

        (data_frame())
            ->read(from_array([
                ['id' => 1, 'group_id' => 1, 'name' => 'Group 1 - Item 1'],
                ['id' => 2, 'group_id' => 1, 'name' => 'Group 1 - Item 2'],
                ['id' => 1, 'group_id' => 2, 'name' => 'Group 2 - Item 1'],
                ['id' => 2, 'group_id' => 2, 'name' => 'Group 2 - Item 2'],
            ]))
            ->load(to_dbal_table_insert($this->mysqlConnectionParams(), $table))
            ->run();

        self::assertEquals(4, $this->mysqlDatabaseContext->tableCount($table));

        (data_frame())
            ->read(from_array([
                ['id' => 1, 'group_id' => 2],
                ['id' => 2, 'group_id' => 1],
            ]))
            ->load(to_dbal_table_delete($this->mysqlConnectionParams(), $table))
            ->run();

        self::assertEquals(2, $this->mysqlDatabaseContext->tableCount($table));
        self::assertEquals(
            [
                ['id' => 1, 'group_id' => 1, 'name' => 'Group 1 - Item 1'],
                ['id' => 2, 'group_id' => 2, 'name' => 'Group 2 - Item 2'],
            ],
            $this->mysqlDatabaseContext->selectAll($table)
        );
    }

    public function test_deletes_rows_by_single_key() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        // Insert test data
        (data_frame())
            ->read(from_array([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ['id' => 4, 'name' => 'Name Four', 'description' => 'Description Four'],
            ]))
            ->load(to_dbal_table_insert($this->mysqlConnectionParams(), $table))
            ->run();

        self::assertEquals(4, $this->mysqlDatabaseContext->tableCount($table));

        (data_frame())
            ->read(from_array([
                ['id' => 2],
                ['id' => 4],
            ]))
            ->load(to_dbal_table_delete($this->mysqlConnectionParams(), $table))
            ->run();

        self::assertEquals(2, $this->mysqlDatabaseContext->tableCount($table));
        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ],
            $this->mysqlDatabaseContext->selectAll($table)
        );
    }

    public function test_inserts_empty_rows() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $loader = to_dbal_table_insert($this->mysqlConnectionParams(), $table);

        (data_frame())
            ->read(from_array([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ]))
            ->collect()
            ->filter(ref('id')->equals(0))
            ->load($loader)
            ->run();

        self::assertEquals(0, $this->mysqlDatabaseContext->tableCount($table));
    }

    public function test_inserts_multiple_rows_at_once() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $loader = to_dbal_table_insert($this->mysqlConnectionParams(), $table);

        (data_frame())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                    ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                    ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ])
            )
            ->load($loader)
            ->run();

        self::assertEquals(3, $this->mysqlDatabaseContext->tableCount($table));
    }

    public function test_inserts_multiple_rows_at_once_using_existing_connection() : void
    {
        $this->mysqlDatabaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                ],
            ))
                ->setPrimaryKey(['id'])
        );

        $loader = to_dbal_table_insert($this->mysqlDatabaseContext->connection(), $table);

        (data_frame())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                    ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                    ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ])
            )
            ->load($loader)
            ->run();

        self::assertEquals(3, $this->mysqlDatabaseContext->tableCount($table));
        self::assertEquals(1, $this->mysqlDatabaseContext->numberOfExecutedInsertQueries());
    }

    public function test_inserts_multiple_rows_in_two_insert_queries() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        (data_frame())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                    ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                    ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ])
            )
            ->load(to_dbal_table_insert($this->mysqlConnectionParams(), $table))
            ->run();

        self::assertEquals(3, $this->mysqlDatabaseContext->tableCount($table));
    }

    public function test_inserts_new_rows_and_skip_already_existed() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));
        (data_frame())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                    ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                    ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ])
            )
            ->load(to_dbal_table_insert($this->mysqlConnectionParams(), $table))
            ->run();

        (data_frame())
            ->read(
                from_array([
                    ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                    ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                    ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
                ])
            )
            ->load(to_dbal_table_insert($this->mysqlConnectionParams(), $table, MySQLInsertOptions::new()->skipConflicts()))
            ->run();

        self::assertEquals(4, $this->mysqlDatabaseContext->tableCount($table));
        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            ],
            $this->mysqlDatabaseContext->selectAll($table)
        );
    }

    public function test_inserts_xml_element_entry() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $loader = to_dbal_table_insert($this->mysqlConnectionParams(), $table);

        $documentA = new \DOMDocument();
        $documentA->loadXml('<xml>Description One</xml>');

        $documentB = new \DOMDocument();
        $documentB->loadXml('<xml>Description Two</xml>');

        $documentC = new \DOMDocument();
        $documentC->loadXml('<xml>Description Three</xml>');

        (data_frame())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Name One', 'description' => $documentA->getElementsByTagName('xml')[0]],
                    ['id' => 2, 'name' => 'Name Two', 'description' => $documentB->getElementsByTagName('xml')[0]],
                    ['id' => 3, 'name' => 'Name Three', 'description' => $documentC->getElementsByTagName('xml')[0]],
                ]),
            )
            ->load($loader)
            ->run();

        self::assertEquals(3, $this->mysqlDatabaseContext->tableCount($table));
        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => '<xml>Description One</xml>'],
                ['id' => 2, 'name' => 'Name Two', 'description' => '<xml>Description Two</xml>'],
                ['id' => 3, 'name' => 'Name Three', 'description' => '<xml>Description Three</xml>'],
            ],
            $this->mysqlDatabaseContext->selectAll($table)
        );
    }

    public function test_inserts_xml_entry() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $loader = to_dbal_table_insert($this->mysqlConnectionParams(), $table);

        $documentA = new \DOMDocument();
        $documentA->loadXml('<xml>Description One</xml>');

        $documentB = new \DOMDocument();
        $documentB->loadXml('<xml>Description Two</xml>');

        $documentC = new \DOMDocument();
        $documentC->loadXml('<b>Description Three</b>');

        (data_frame())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Name One', 'description' => $documentA],
                    ['id' => 2, 'name' => 'Name Two', 'description' => $documentB],
                    ['id' => 3, 'name' => 'Name Three', 'description' => $documentC],
                ]),
            )
            ->load($loader)
            ->run();

        self::assertEquals(3, $this->mysqlDatabaseContext->tableCount($table));
        self::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => '<xml>Description One</xml>'],
                ['id' => 2, 'name' => 'Name Two', 'description' => '<xml>Description Two</xml>'],
                ['id' => 3, 'name' => 'Name Three', 'description' => '<b>Description Three</b>'],
            ],
            $this->mysqlDatabaseContext->selectAll($table)
        );
    }

    public function test_update_multiple_rows_at_once() : void
    {
        $this->mysqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $insertLoader = to_dbal_table_insert($this->mysqlConnectionParams(), $table);
        $updateLoader = to_dbal_table_update($this->mysqlConnectionParams(), $table, MySQLUpdateOptions::new());

        (data_frame())->extract(
            from_array([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ])
        )
            ->load($insertLoader)
            ->run();

        (data_frame())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'Changed Name One', 'description' => 'Description One'],
                    ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                    ['id' => 3, 'name' => 'Changed Name Three', 'description' => 'Description Three'],
                ])
            )
            ->load($updateLoader)
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Changed Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Changed Name Three', 'description' => 'Description Three'],
            ],
            $this->mysqlDatabaseContext->selectAll('flow_doctrine_bulk_test')
        );

        self::assertEquals(3, $this->mysqlDatabaseContext->tableCount($table));
    }
}
