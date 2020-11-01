<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Integration;

use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\BulkInsert;
use Flow\Doctrine\Bulk\Tests\IntegrationTestCase;

final class PostgreSqlBulkInsertTest extends IntegrationTestCase
{
    public function test_inserts_multiple_rows_at_once() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_doctrine_bulk_test');
        $bulkInsert = BulkInsert::create($this->pgsqlDatabaseContext->connection());

        $bulkInsert->insert(
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ])
        );

        $this->assertEquals(3, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(1, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());
    }

    public function test_inserts_new_rows_and_skip_already_existed() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_doctrine_bulk_test');
        $bulkInsert = BulkInsert::create($this->pgsqlDatabaseContext->connection());
        $bulkInsert->insert(
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ])
        );

        $bulkInsert->insertOrSkipOnConflict(
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            ])
        );

        $this->assertEquals(4, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(2, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            ],
            $this->pgsqlDatabaseContext->selectAll($table)
        );
    }

    public function test_inserts_new_rows_or_updates_already_existed_based_on_primary_key() : void
    {
        $this->pgsqlDatabaseContext->createTestTable($table = 'flow_doctrine_bulk_test');
        $bulkInsert = BulkInsert::create($this->pgsqlDatabaseContext->connection());
        $bulkInsert->insert(
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ])
        );

        $bulkInsert->insertOrUpdateOnConstraintConflict(
            $table,
            'flow_doctrine_bulk_test_pkey',
            new BulkData([
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            ])
        );

        $this->assertEquals(4, $this->pgsqlDatabaseContext->tableCount($table));
        $this->assertEquals(2, $this->pgsqlDatabaseContext->numberOfExecutedInsertQueries());
        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'New Name Two', 'description' => 'New Description Two'],
                ['id' => 3, 'name' => 'New Name Three', 'description' => 'New Description Three'],
                ['id' => 4, 'name' => 'New Name Four', 'description' => 'New Description Three'],
            ],
            $this->pgsqlDatabaseContext->selectAll($table)
        );
    }
}
