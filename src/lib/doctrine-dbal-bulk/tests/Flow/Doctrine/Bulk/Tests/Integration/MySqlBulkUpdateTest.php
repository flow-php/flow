<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Integration;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Tests\MysqlIntegrationTestCase;

final class MySqlBulkUpdateTest extends MysqlIntegrationTestCase
{
    public function test_update_multiple_rows_with_all_columns_at_once() : void
    {
        $this->mysqlDatabaseContext->createTable(
            (new Table(
                $table = 'flow_doctrine_bulk_test',
                [
                    new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                    new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                    new Column('active', Type::getType(Types::BOOLEAN), ['notnull' => true]),
                ],
            ))
            ->setPrimaryKey(['id'])
        );

        Bulk::create()->insert(
            $this->mysqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two', 'active' => true],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three', 'active' => false],
            ])
        );

        Bulk::create()->update(
            $this->mysqlDatabaseContext->connection(),
            $table,
            new BulkData([
                ['id' => 2, 'name' => 'Changed name Two', 'description' => 'Changed description Two', 'active' => false],
                ['id' => 3, 'name' => 'Changed name Three', 'description' => 'Changed description Three', 'active' => true],
            ]),
        );

        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One', 'active' => false],
                ['id' => 2, 'name' => 'Changed name Two', 'description' => 'Changed description Two', 'active' => false],
                ['id' => 3, 'name' => 'Changed name Three', 'description' => 'Changed description Three', 'active' => true],
            ],
            $this->mysqlDatabaseContext->selectAll($table)
        );
    }
}
