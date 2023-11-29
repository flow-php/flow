<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use function Flow\ETL\Adapter\Doctrine\dbal_from_queries;
use function Flow\ETL\Adapter\Doctrine\dbal_from_query;
use function Flow\ETL\DSL\from_array;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\ParametersSet;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Flow;

final class DbalQueryExtractorTest extends IntegrationTestCase
{
    public function test_extracting_multiple_rows_at_once() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        (new Flow())->extract(
            from_array([
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ])
        )->load(
            DbalLoader::fromConnection($this->pgsqlDatabaseContext->connection(), $table)
        )->run();

        $rows = (new Flow())->extract(
            dbal_from_query(
                $this->pgsqlDatabaseContext->connection(),
                "SELECT * FROM {$table} ORDER BY id"
            )
        )->fetch();

        $this->assertSame(
            [
                ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
                ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
                ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
            ],
            $rows->toArray()
        );
    }

    public function test_extracting_multiple_rows_multiple_times() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            $table = 'flow_doctrine_bulk_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
            ->setPrimaryKey(['id']));

        (new Flow())
            ->extract(
                from_array([
                    ['id' => 1, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 2, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 3, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 4, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 5, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 6, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 7, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 8, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 9, 'name' => 'Name', 'description' => 'Description'],
                    ['id' => 10, 'name' => 'Name', 'description' => 'Description'],
                ])
            )
            ->load(DbalLoader::fromConnection($this->pgsqlDatabaseContext->connection(), $table))
            ->run();

        $rows = (new Flow())->extract(
            dbal_from_queries(
                $this->pgsqlDatabaseContext->connection(),
                "SELECT * FROM {$table} ORDER BY id LIMIT :limit OFFSET :offset",
                new ParametersSet(
                    ['limit' => 2, 'offset' => 0],
                    ['limit' => 2, 'offset' => 2],
                    ['limit' => 2, 'offset' => 4],
                    ['limit' => 2, 'offset' => 6],
                    ['limit' => 2, 'offset' => 8],
                )
            )
        )->fetch();

        $this->assertSame(10, $rows->count());
        $this->assertSame(
            [
                ['id' => 1, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 2, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 3, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 4, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 5, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 6, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 7, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 8, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 9, 'name' => 'Name', 'description' => 'Description'],
                ['id' => 10, 'name' => 'Name', 'description' => 'Description'],
            ],
            $rows->toArray()
        );
    }
}
