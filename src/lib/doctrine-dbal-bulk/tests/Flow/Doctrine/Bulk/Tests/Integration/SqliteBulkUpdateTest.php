<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Integration;

use Doctrine\DBAL\Exception\NotNullConstraintViolationException;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Tests\Mother\WideTableMother;
use Flow\Doctrine\Bulk\Tests\SqliteIntegrationTestCase;

final class SqliteBulkUpdateTest extends SqliteIntegrationTestCase
{
    public function test_a_failing_chunk_rolls_back_the_whole_update(): void
    {
        $this->databaseContext->createTable(WideTableMother::tableWithPrimaryKey(
            $table = 'flow_doctrine_bulk_rollback_test',
            40,
        ));
        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData(WideTableMother::rows(40, 1000)),
        );

        $rows = WideTableMother::changedRows(40, 1000);

        // 819 rows fit under the cap at 40 columns, so row 1000 sits in the second of two statements
        $rows[999]['c2'] = null;

        $failure = null;

        try {
            Bulk::create()->update($this->databaseContext->connection(), $table, new BulkData($rows));
        } catch (NotNullConstraintViolationException $e) {
            $failure = $e;
        }

        static::assertInstanceOf(NotNullConstraintViolationException::class, $failure);
        static::assertEquals(WideTableMother::rows(40, 1000), $this->databaseContext->selectAll($table, 'c1'));
    }

    public function test_update_multiple_rows_with_all_columns_at_once(): void
    {
        // @mago-expect analysis:deprecated-method
        $this->databaseContext->createTable((new Table($table = 'flow_doctrine_bulk_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
        ]))->setPrimaryKey(['id']));

        Bulk::create()->insert($this->databaseContext->connection(), $table, new BulkData([
            ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
            ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
            ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false],
        ]));

        Bulk::create()->update($this->databaseContext->connection(), $table, new BulkData([
            ['id' => 2, 'name' => 'Changed name Two', 'description' => 'Changed description Two', 'active' => false],
            ['id' => 3, 'name' => 'Changed name Three', 'description' => 'Changed description Three', 'active' => true],
        ]));

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
                [
                    'id' => 2,
                    'name' => 'Changed name Two',
                    'description' => 'Changed description Two',
                    'active' => false,
                ],
                [
                    'id' => 3,
                    'name' => 'Changed name Three',
                    'description' => 'Changed description Three',
                    'active' => true,
                ],
            ],
            $this->databaseContext->selectAll($table),
        );
    }

    public function test_update_of_more_rows_than_the_bind_cap_succeeds(): void
    {
        $this->databaseContext->createTable(WideTableMother::tableWithPrimaryKey(
            $table = 'flow_doctrine_bulk_wide_test',
            40,
        ));
        Bulk::create()->insert(
            $this->databaseContext->connection(),
            $table,
            new BulkData(WideTableMother::rows(40, 1000)),
        );

        // 1000 rows x 40 columns = 40 000 parameters, past SQLite's 32 766
        Bulk::create()->update(
            $this->databaseContext->connection(),
            $table,
            new BulkData(WideTableMother::changedRows(40, 1000)),
        );

        static::assertEquals(WideTableMother::changedRows(40, 1000), $this->databaseContext->selectAll($table, 'c1'));
    }
}
