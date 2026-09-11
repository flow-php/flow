<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Dialects;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\Doctrine\Order;
use Flow\ETL\Adapter\Doctrine\OrderBy;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;

use function Flow\ETL\Adapter\Doctrine\from_dbal_limit_offset;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SqliteSchemaDerivationTest extends IntegrationTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this->sqliteDatabaseContext->createTable(
            (new Table('orders', [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('name', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
                new Column('amount', Type::getType(Types::FLOAT), ['notnull' => true]),
            ]))->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create()),
        );

        for ($i = 1; $i <= 3; $i++) {
            $this->sqliteDatabaseContext->insert('orders', ['id' => $i, 'name' => 'name_' . $i, 'amount' => $i * 1.5]);
        }
    }

    public function test_reads_inside_from_all(): void
    {
        $rows = data_frame()
            ->read(from_all(
                from_dbal_limit_offset(
                    $this->sqliteDatabaseContext->connection(),
                    'orders',
                    new OrderBy('id', Order::ASC),
                ),
                from_array([['id' => '99', 'name' => 'name_99', 'amount' => '1.5']]),
            ))
            ->fetch();

        static::assertCount(4, $rows);
        static::assertSame(['1', '2', '3', '99'], $rows->reduceToArray('id'));
    }

    public function test_schema_is_all_string_and_extract_agrees(): void
    {
        $extractor = from_dbal_limit_offset(
            $this->sqliteDatabaseContext->connection(),
            'orders',
            new OrderBy('id', Order::ASC),
            2,
        );

        static::assertEquals(
            schema(str_schema('id', true), str_schema('name', true), str_schema('amount', true)),
            $extractor->schema(),
        );

        $values = [];

        foreach ($extractor->extract(flow_context()) as $rows) {
            static::assertTrue($rows->schema()->isSame($extractor->schema()));
            $values = [...$values, ...$rows->toArray()];
        }

        static::assertSame(
            [
                ['id' => '1', 'name' => 'name_1', 'amount' => '1.5'],
                ['id' => '2', 'name' => 'name_2', 'amount' => '3'],
                ['id' => '3', 'name' => 'name_3', 'amount' => '4.5'],
            ],
            $values,
        );
    }
}
