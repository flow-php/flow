<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table as DoctrineTable;
use Doctrine\DBAL\Types\IntegerType as DoctrineIntegerType;
use Doctrine\DBAL\Types\TextType;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\Order;
use Flow\ETL\Adapter\Doctrine\OrderBy;
use Flow\ETL\Adapter\Doctrine\Table;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Adapter\Doctrine\TypesMap;
use Flow\ETL\Config;
use Flow\ETL\Rows;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function array_map;
use function Flow\ETL\Adapter\Doctrine\from_dbal_limit_offset;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function iterator_to_array;

final class DbalLimitOffsetExtractorTest extends IntegrationTestCase
{
    public function test_creating_limit_offset_extractor_for_table(): void
    {
        $this->pgsqlDatabaseContext->createTable((new DoctrineTable($table = 'flow_doctrine_order_by_test', [
            new Column('id', Type::getType(Types::INTEGER), ['notnull' => true]),
            new Column('code', Type::getType(Types::INTEGER), ['notnull' => true]),
        ]))->addPrimaryKeyConstraint(PrimaryKeyConstraint::editor()->setUnquotedColumnNames('id')->create()));

        $customTypesMap = new TypesMap([
            StringType::class => TextType::class,
            IntegerType::class => DoctrineIntegerType::class,
        ]);

        $loader = (new DbalLoader($table, $this->postgresqlConnectionParams()))->withTypesMap($customTypesMap);

        data_frame()
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
            ],
        );

        static::assertSame(
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
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_creating_limit_offset_extractor_for_table_without_order_by(): void
    {
        $this->expectExceptionMessage('There must be at least one column to order by, zero given');

        from_dbal_limit_offset($this->pgsqlDatabaseContext->connection(), new Table('table', ['id', 'name']), []);
    }
}
