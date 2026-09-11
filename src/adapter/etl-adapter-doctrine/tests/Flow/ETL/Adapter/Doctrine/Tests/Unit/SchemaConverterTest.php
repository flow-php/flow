<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Adapter\Doctrine\DbalMetadata;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Doctrine\table_schema_to_flow_schema;
use function Flow\ETL\Adapter\Doctrine\to_dbal_schema_table;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function method_exists;

final class SchemaConverterTest extends FlowTestCase
{
    public function test_converting_flow_to_dbal_schema(): void
    {
        $flowSchema = schema(
            int_schema('int', nullable: false, metadata: DbalMetadata::primaryKey('pk_test')),
            str_schema('str', nullable: true, metadata: DbalMetadata::primaryKey('pk_test')),
            int_schema('bigint', nullable: false, metadata: DbalMetadata::type('bigint')),
            str_schema('str_with_length', true, DbalMetadata::length(255)),
            str_schema('str_unique', true, DbalMetadata::indexUnique('idx_str_unique')),
            date_schema('date', nullable: true, metadata: DbalMetadata::index('idx_date')),
            float_schema('float', nullable: true, metadata: DbalMetadata::precision(10)->merge(DbalMetadata::scale(2))),
            float_schema('float_default'),
            bool_schema('bool', nullable: true, metadata: DbalMetadata::default(true)),
            json_schema('json', nullable: true, metadata: DbalMetadata::platformOptions(['jsonb' => true])),
            list_schema('list', type_list(type_integer()), metadata: DbalMetadata::columnDefinition('integer[]')),
            map_schema(
                'map',
                type_map(type_integer(), type_string()),
                metadata: DbalMetadata::comment('test comment!'),
            ),
        );

        $expectedTable = new Table(
            'test',
            [
                new Column('int', Type::getType('integer'), ['notnull' => true]),
                new Column('str', Type::getType('string'), ['notnull' => true]),
                new Column('bigint', Type::getType('bigint'), ['notnull' => true]),
                new Column('str_with_length', Type::getType('string'), ['notnull' => false, 'length' => 255]),
                new Column('str_unique', Type::getType('string'), ['notnull' => false]),
                new Column('float', Type::getType('float'), ['notnull' => false, 'precision' => 10, 'scale' => 2]),
                new Column('float_default', Type::getType('float'), ['notnull' => true]),
                new Column('bool', Type::getType('boolean'), ['notnull' => false, 'default' => true]),
                new Column('json', Type::getType('json'), [
                    'notnull' => false,
                    'platformOptions' => ['jsonb' => true],
                ]),
                new Column('list', Type::getType('json'), ['notnull' => true, 'columnDefinition' => 'integer[]']),
                new Column('map', Type::getType('json'), ['notnull' => true, 'comment' => 'test comment!']),
                new Column('date', Type::getType('date_immutable'), ['notnull' => false]),
            ],
            [
                new Index('idx_date', ['date'], false, false),
                new Index('idx_str_unique', ['str_unique'], true, false),
            ],
        );
        $expectedTable->addPrimaryKeyConstraint(
            PrimaryKeyConstraint::editor()->setUnquotedName('pk_test')->setUnquotedColumnNames('int', 'str')->create(),
        );

        static::assertEquals($expectedTable, to_dbal_schema_table($flowSchema, 'test'));
    }

    public function test_converting_flow_to_dbal_schema_without_providing_pk_name(): void
    {
        $flowSchema = schema(
            int_schema('int', nullable: false, metadata: DbalMetadata::primaryKey()),
            str_schema('str', nullable: true, metadata: DbalMetadata::primaryKey()),
        );

        $expectedTable = new Table('test', [
            new Column('int', Type::getType('integer'), ['notnull' => true]),
            new Column('str', Type::getType('string'), ['notnull' => true]),
        ]);
        $expectedTable->addPrimaryKeyConstraint(
            PrimaryKeyConstraint::editor()->setUnquotedColumnNames('int', 'str')->create(),
        );

        static::assertEquals($expectedTable, to_dbal_schema_table($flowSchema, 'test'));
    }

    public function test_dbal_schema_to_flow_schema_dbal_36(): void
    {
        // changeColumn was removed in doctrine/dbal 4.0
        // We are using it to perform a different assertion since prior to 4.0 all
        // columns were also getting precision set to 10 due to a bug that was executing precision set
        // even when precision value was null.
        if (!method_exists(Table::class, 'changeColumn')) {
            static::markTestSkipped('Doctrine DBAL >= 3.6+ < 4.0');
        }

        $inputTable = new Table(
            'test',
            [
                new Column('int', Type::getType('integer'), ['notnull' => true]),
                new Column('str', Type::getType('string'), ['notnull' => true]),
                new Column('bigint', Type::getType('bigint'), ['notnull' => true]),
                new Column('str_with_length', Type::getType('string'), ['notnull' => false, 'length' => 255]),
                new Column('str_unique', Type::getType('string'), ['notnull' => false]),
                new Column('float', Type::getType('float'), [
                    'notnull' => false,
                    'precision' => 10,
                    'scale' => 2,
                ]),
                new Column('float_default', Type::getType('float'), ['notnull' => true, 'scale' => 6]),
                new Column('bool', Type::getType('boolean'), ['notnull' => false, 'default' => true]),
                new Column('json', Type::getType('json'), [
                    'notnull' => false,
                    'platformOptions' => ['jsonb' => true],
                ]),
                new Column('list', Type::getType('json'), [
                    'notnull' => true,
                    'columnDefinition' => 'integer[]',
                ]),
                new Column('map', Type::getType('json'), ['notnull' => true, 'comment' => 'test comment!']),
                new Column('date', Type::getType('date_immutable'), ['notnull' => false]),
            ],
            [
                new Index('idx_date', ['date'], false, false),
                new Index('idx_str_unique', ['str_unique'], true, false),
            ],
        );
        $inputTable->addPrimaryKeyConstraint(
            PrimaryKeyConstraint::editor()->setUnquotedName('pk_test')->setUnquotedColumnNames('int', 'str')->create(),
        );

        static::assertEquals(
            schema(
                int_schema(
                    'int',
                    nullable: false,
                    metadata: DbalMetadata::primaryKey('pk_test')->merge(DbalMetadata::precision(10)),
                ),
                str_schema(
                    'str',
                    nullable: false,
                    metadata: DbalMetadata::primaryKey('pk_test')->merge(DbalMetadata::precision(10)),
                ),
                int_schema('bigint', nullable: false, metadata: DbalMetadata::precision(10)),
                str_schema('str_with_length', true, DbalMetadata::length(255)->merge(DbalMetadata::precision(10))),
                str_schema(
                    'str_unique',
                    true,
                    DbalMetadata::indexUnique('idx_str_unique')->merge(DbalMetadata::precision(10)),
                ),
                date_schema(
                    'date',
                    nullable: true,
                    metadata: DbalMetadata::index('idx_date')->merge(DbalMetadata::precision(10)),
                ),
                float_schema(
                    'float',
                    nullable: true,
                    metadata: DbalMetadata::precision(10)
                        ->merge(DbalMetadata::scale(2))
                        ->merge(DbalMetadata::precision(10)),
                ),
                float_schema('float_default', metadata: DbalMetadata::scale(6)->merge(DbalMetadata::precision(10))),
                bool_schema(
                    'bool',
                    nullable: true,
                    metadata: DbalMetadata::default(true)->merge(DbalMetadata::precision(10)),
                ),
                json_schema(
                    'json',
                    nullable: true,
                    metadata: DbalMetadata::platformOptions(['jsonb' => true])->merge(DbalMetadata::precision(10)),
                ),
                json_schema(
                    'list',
                    metadata: DbalMetadata::columnDefinition('integer[]')->merge(DbalMetadata::precision(10)),
                ),
                json_schema(
                    'map',
                    metadata: DbalMetadata::comment('test comment!')->merge(DbalMetadata::precision(10)),
                ),
            ),
            table_schema_to_flow_schema($inputTable),
        );
    }

    public function test_dbal_schema_to_flow_schema_dbal_40(): void
    {
        // changeColumn was removed in doctrine/dbal 4.0
        // We are using it to perform a different assertion since prior to 4.0 all
        // columns were also getting precision set to 10 due to a bug that was executing precision set
        // even when precision value was null.
        if (method_exists(Table::class, 'changeColumn')) {
            static::markTestSkipped('Doctrine DBAL >= 4.0+');
        }

        $inputTable = new Table(
            'test',
            [
                new Column('int', Type::getType('integer'), ['notnull' => true]),
                new Column('str', Type::getType('string'), ['notnull' => true]),
                new Column('bigint', Type::getType('bigint'), ['notnull' => true]),
                new Column('str_with_length', Type::getType('string'), ['notnull' => false, 'length' => 255]),
                new Column('str_unique', Type::getType('string'), ['notnull' => false]),
                new Column('float', Type::getType('float'), [
                    'notnull' => false,
                    'precision' => 10,
                    'scale' => 2,
                ]),
                new Column('float_default', Type::getType('float'), ['notnull' => true, 'scale' => 6]),
                new Column('bool', Type::getType('boolean'), ['notnull' => false, 'default' => true]),
                new Column('json', Type::getType('json'), [
                    'notnull' => false,
                    'platformOptions' => ['jsonb' => true],
                ]),
                new Column('list', Type::getType('json'), [
                    'notnull' => true,
                    'columnDefinition' => 'integer[]',
                ]),
                new Column('map', Type::getType('json'), ['notnull' => true, 'comment' => 'test comment!']),
                new Column('date', Type::getType('date_immutable'), ['notnull' => false]),
            ],
            [
                new Index('idx_date', ['date'], false, false),
                new Index('idx_str_unique', ['str_unique'], true, false),
            ],
        );
        $inputTable->addPrimaryKeyConstraint(
            PrimaryKeyConstraint::editor()->setUnquotedName('pk_test')->setUnquotedColumnNames('int', 'str')->create(),
        );

        static::assertEquals(
            schema(
                int_schema('int', nullable: false, metadata: DbalMetadata::primaryKey('pk_test')),
                str_schema('str', nullable: false, metadata: DbalMetadata::primaryKey('pk_test')),
                int_schema('bigint', nullable: false),
                str_schema('str_with_length', true, DbalMetadata::length(255)),
                str_schema('str_unique', true, DbalMetadata::indexUnique('idx_str_unique')),
                date_schema('date', nullable: true, metadata: DbalMetadata::index('idx_date')),
                float_schema(
                    'float',
                    nullable: true,
                    metadata: DbalMetadata::precision(10)->merge(DbalMetadata::scale(2)),
                ),
                float_schema('float_default', metadata: DbalMetadata::scale(6)),
                bool_schema('bool', nullable: true, metadata: DbalMetadata::default(true)),
                json_schema('json', nullable: true, metadata: DbalMetadata::platformOptions(['jsonb' => true])),
                json_schema('list', metadata: DbalMetadata::columnDefinition('integer[]')),
                json_schema('map', metadata: DbalMetadata::comment('test comment!')),
            ),
            table_schema_to_flow_schema($inputTable),
        );
    }

    public function test_dbal_schema_to_flow_schema_drops_null_platform_options(): void
    {
        static::assertEquals(schema(int_schema('id', nullable: false)), table_schema_to_flow_schema(new Table('t', [
            new Column('id', Type::getType('integer'), [
                'notnull' => true,
                'platformOptions' => ['charset' => null, 'collation' => null],
            ]),
        ])));
    }
}
