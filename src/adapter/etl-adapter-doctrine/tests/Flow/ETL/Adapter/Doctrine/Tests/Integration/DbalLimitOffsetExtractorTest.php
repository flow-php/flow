<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use function Flow\ETL\Adapter\Doctrine\from_dbal_limit_offset;
use function Flow\ETL\DSL\{data_frame, flow_context, from_array};
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Types\{TextType, Type, Types};
use Flow\ETL\Adapter\Doctrine\{DbalLoader, DbalTypesDetector, Order, OrderBy, Table, TypesMap};
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\{Config, Rows};
use Flow\Types\Type\Native\{IntegerType, StringType};

final class DbalLimitOffsetExtractorTest extends IntegrationTestCase
{
    public function test_creating_limit_offset_extractor_for_table() : void
    {
        $this->pgsqlDatabaseContext->createTable((new \Doctrine\DBAL\Schema\Table(
            $table = 'flow_doctrine_order_by_test',
            [
                new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
                new Column('code', Type::getType(Types::INTEGER), ['notnull' => true]),
            ],
        ))
            ->setPrimaryKey(['id']));

        $customTypesMap = new TypesMap([
            StringType::class => TextType::class,
            IntegerType::class => \Doctrine\DBAL\Types\IntegerType::class,
        ]);

        $customConverter = new DbalTypesDetector($customTypesMap);

        $loader = (new DbalLoader($table, $this->postgresqlConnectionParams()))
            ->withTypesDetector($customConverter);

        (data_frame())
            ->read(from_array([
                ['id' => 1, 'code' => 100],
                ['id' => 2, 'code' => 100],
                ['id' => 3, 'code' => 200],
            ]))
            ->load($loader)
            ->run();

        $extractor = from_dbal_limit_offset(
            $this->pgsqlDatabaseContext->connection(),
            new Table('flow_doctrine_order_by_test', ['id', 'code']),
            [
                new OrderBy('code', Order::DESC),
                new OrderBy('id', Order::ASC),
            ]
        );

        self::assertSame(
            [
                [
                    [
                        'id' => 3,
                        'code' => 200,
                    ],
                ],
                [
                    [
                        'id' => 1,
                        'code' => 100,
                    ],
                ],
                [
                    [
                        'id' => 2,
                        'code' => 100,
                    ],
                ],
            ],
            \array_map(
                static fn (Rows $r) => $r->toArray(),
                \iterator_to_array($extractor->extract(flow_context(Config::builder()->putInputIntoRows()->build())))
            )
        );
    }

    public function test_creating_limit_offset_extractor_for_table_without_order_by() : void
    {
        $this->expectExceptionMessage('There must be at least one column to order by, zero given');

        from_dbal_limit_offset(
            $this->pgsqlDatabaseContext->connection(),
            new Table('table', ['id', 'name']),
            []
        );
    }
}
