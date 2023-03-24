<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use function Flow\ETL\DSL\ref;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\Doctrine\DbalDataFrameFactory;
use Flow\ETL\Adapter\Doctrine\Parameter;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class DbalDataFrameFactoryTest extends IntegrationTestCase
{
    public function test_create_loader_with_invalid_operation() : void
    {
        $this->pgsqlDatabaseContext->createTable((new Table(
            'flow_doctrine_data_factory_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('description', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
            ],
        ))
        ->setPrimaryKey(['id']));

        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 1, 'name' => 'Name 1', 'description' => 'Some Description 1']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 2, 'name' => 'Name 2', 'description' => 'Some Description 2']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 3, 'name' => 'Name 3', 'description' => 'Some Description 3']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 4, 'name' => 'Name 4', 'description' => 'Some Description 4']);
        $this->pgsqlDatabaseContext->insert('flow_doctrine_data_factory_test', ['id' => 5, 'name' => 'Name 5', 'description' => 'Some Description 5']);

        $rows = (
            new DbalDataFrameFactory(
                $this->connectionParams(),
                'SELECT * FROM flow_doctrine_data_factory_test WHERE id IN (:ids)',
                Parameter::ints('ids', ref('id'))
            )
        )
        ->from(new Rows(
            Row::with(Entry::int('id', 1)),
            Row::with(Entry::int('id', 2)),
            Row::with(Entry::int('id', 3)),
            Row::with(Entry::int('id', 55)),
        ))
        ->select('id')
        ->fetch();

        $this->assertSame(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            $rows->toArray()
        );
    }
}
