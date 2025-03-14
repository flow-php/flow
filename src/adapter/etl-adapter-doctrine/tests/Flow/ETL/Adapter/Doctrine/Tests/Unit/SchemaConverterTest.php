<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use function Flow\ETL\Adapter\Doctrine\{table_schema_to_flow_schema, to_dbal_schema_table};
use function Flow\ETL\DSL\{bool_schema,
    date_schema,
    float_schema,
    int_schema,
    json_schema,
    list_schema,
    map_schema,
    schema,
    str_schema,
    type_integer,
    type_list,
    type_map,
    type_string};
use Doctrine\DBAL\Schema\{Column, Index, Table};
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Adapter\Doctrine\DbalMetadata;
use Flow\ETL\Tests\FlowTestCase;

final class SchemaConverterTest extends FlowTestCase
{
    public function test_converting_flow_to_dbal_schema() : void
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
            map_schema('map', type_map(type_integer(), type_string()), metadata: DbalMetadata::comment('test comment!')),
        );

        self::assertEquals(
            new Table(
                'test',
                [
                    new Column('int', Type::getType('integer'), ['notnull' => true]),
                    new Column('str', Type::getType('string'), ['notnull' => true]), // pk changes nullable true into false
                    new Column('bigint', Type::getType('bigint'), ['notnull' => true]),
                    new Column('str_with_length', Type::getType('string'), ['notnull' => false, 'length' => 255]),
                    new Column('str_unique', Type::getType('string'), ['notnull' => false]),
                    new Column('float', Type::getType('float'), ['notnull' => false, 'precision' => 10, 'scale' => 2]),
                    new Column('float_default', Type::getType('float'), ['notnull' => true, 'scale' => 6]),
                    new Column('bool', Type::getType('boolean'), ['notnull' => false, 'default' => true]),
                    new Column('json', Type::getType('json'), ['notnull' => false, 'platformOptions' => ['jsonb' => true]]),
                    new Column('list', Type::getType('json'), ['notnull' => true, 'columnDefinition' => 'integer[]']),
                    new Column('map', Type::getType('json'), ['notnull' => true, 'comment' => 'test comment!']),
                    new Column('date', Type::getType('date_immutable'), ['notnull' => false]),
                ],
                [
                    new Index('pk_test', ['int', 'str'], true, true),
                    new Index('idx_date', ['date'], false, false),
                    new Index('idx_str_unique', ['str_unique'], true, false),
                ]
            ),
            to_dbal_schema_table($flowSchema, 'test')
        );
    }

    public function test_converting_flow_to_dbal_schema_without_providing_pk_name() : void
    {
        $flowSchema = schema(
            int_schema('int', nullable: false, metadata: DbalMetadata::primaryKey()),
            str_schema('str', nullable: true, metadata: DbalMetadata::primaryKey()),
        );

        self::assertEquals(
            new Table(
                'test',
                [
                    new Column('int', Type::getType('integer'), ['notnull' => true]),
                    new Column('str', Type::getType('string'), ['notnull' => true]), // pk changes nullable true into false
                ],
                [
                    new Index('', ['int', 'str'], true, true),
                ]
            ),
            to_dbal_schema_table($flowSchema, 'test')
        );
    }

    public function test_dbal_schema_to_flow_schema_dbal_36() : void
    {
        // changeColumn was removed in doctrine/dbal 4.0
        // We are using it to perform a different assertion since prior to 4.0 all
        // columns were also getting precision set to 10 due to a bug that was executing precision set
        // even when precision value was null.
        if (!\method_exists(Table::class, 'changeColumn')) {
            self::markTestSkipped('Doctrine DBAL >= 3.6+ < 4.0');
        }

        self::assertEquals(
            schema(
                int_schema('int', nullable: false, metadata: DbalMetadata::primaryKey('pk_test')->merge(DbalMetadata::precision(10))),
                str_schema('str', nullable: false, metadata: DbalMetadata::primaryKey('pk_test')->merge(DbalMetadata::precision(10))),
                int_schema('bigint', nullable: false, metadata: DbalMetadata::precision(10)),
                str_schema('str_with_length', true, DbalMetadata::length(255)->merge(DbalMetadata::precision(10))),
                str_schema('str_unique', true, DbalMetadata::indexUnique('idx_str_unique')->merge(DbalMetadata::precision(10))),
                date_schema('date', nullable: true, metadata: DbalMetadata::index('idx_date')->merge(DbalMetadata::precision(10))),
                float_schema('float', nullable: true, metadata: DbalMetadata::precision(10)->merge(DbalMetadata::scale(2))->merge(DbalMetadata::precision(10))),
                float_schema('float_default', metadata: DbalMetadata::scale(6)->merge(DbalMetadata::precision(10))),
                bool_schema('bool', nullable: true, metadata: DbalMetadata::default(true)->merge(DbalMetadata::precision(10))),
                json_schema('json', nullable: true, metadata: DbalMetadata::platformOptions(['jsonb' => true])->merge(DbalMetadata::precision(10))),
                json_schema('list', metadata: DbalMetadata::columnDefinition('integer[]')->merge(DbalMetadata::precision(10))),
                json_schema('map', metadata: DbalMetadata::comment('test comment!')->merge(DbalMetadata::precision(10))),
            ),
            table_schema_to_flow_schema(
                new Table(
                    'test',
                    [
                        new Column('int', Type::getType('integer'), ['notnull' => true]),
                        new Column('str', Type::getType('string'), ['notnull' => true]), // pk changes nullable true into false
                        new Column('bigint', Type::getType('bigint'), ['notnull' => true]),
                        new Column('str_with_length', Type::getType('string'), ['notnull' => false, 'length' => 255]),
                        new Column('str_unique', Type::getType('string'), ['notnull' => false]),
                        new Column('float', Type::getType('float'), ['notnull' => false, 'precision' => 10, 'scale' => 2]),
                        new Column('float_default', Type::getType('float'), ['notnull' => true, 'scale' => 6]),
                        new Column('bool', Type::getType('boolean'), ['notnull' => false, 'default' => true]),
                        new Column('json', Type::getType('json'), ['notnull' => false, 'platformOptions' => ['jsonb' => true]]),
                        new Column('list', Type::getType('json'), ['notnull' => true, 'columnDefinition' => 'integer[]']),
                        new Column('map', Type::getType('json'), ['notnull' => true, 'comment' => 'test comment!']),
                        new Column('date', Type::getType('date_immutable'), ['notnull' => false]),
                    ],
                    [
                        new Index('pk_test', ['int', 'str'], true, true),
                        new Index('idx_date', ['date'], false, false),
                        new Index('idx_str_unique', ['str_unique'], true, false),
                    ]
                )
            )
        );
    }

    public function test_dbal_schema_to_flow_schema_dbal_40() : void
    {
        // changeColumn was removed in doctrine/dbal 4.0
        // We are using it to perform a different assertion since prior to 4.0 all
        // columns were also getting precision set to 10 due to a bug that was executing precision set
        // even when precision value was null.
        if (\method_exists(Table::class, 'changeColumn')) {
            self::markTestSkipped('Doctrine DBAL >= 4.0+');
        }

        self::assertEquals(
            schema(
                int_schema('int', nullable: false, metadata: DbalMetadata::primaryKey('pk_test')),
                str_schema('str', nullable: false, metadata: DbalMetadata::primaryKey('pk_test')),
                int_schema('bigint', nullable: false),
                str_schema('str_with_length', true, DbalMetadata::length(255)),
                str_schema('str_unique', true, DbalMetadata::indexUnique('idx_str_unique')),
                date_schema('date', nullable: true, metadata: DbalMetadata::index('idx_date')),
                float_schema('float', nullable: true, metadata: DbalMetadata::precision(10)->merge(DbalMetadata::scale(2))),
                float_schema('float_default', metadata: DbalMetadata::scale(6)),
                bool_schema('bool', nullable: true, metadata: DbalMetadata::default(true)),
                json_schema('json', nullable: true, metadata: DbalMetadata::platformOptions(['jsonb' => true])),
                json_schema('list', metadata: DbalMetadata::columnDefinition('integer[]')),
                json_schema('map', metadata: DbalMetadata::comment('test comment!')),
            ),
            table_schema_to_flow_schema(
                new Table(
                    'test',
                    [
                        new Column('int', Type::getType('integer'), ['notnull' => true]),
                        new Column('str', Type::getType('string'), ['notnull' => true]), // pk changes nullable true into false
                        new Column('bigint', Type::getType('bigint'), ['notnull' => true]),
                        new Column('str_with_length', Type::getType('string'), ['notnull' => false, 'length' => 255]),
                        new Column('str_unique', Type::getType('string'), ['notnull' => false]),
                        new Column('float', Type::getType('float'), ['notnull' => false, 'precision' => 10, 'scale' => 2]),
                        new Column('float_default', Type::getType('float'), ['notnull' => true, 'scale' => 6]),
                        new Column('bool', Type::getType('boolean'), ['notnull' => false, 'default' => true]),
                        new Column('json', Type::getType('json'), ['notnull' => false, 'platformOptions' => ['jsonb' => true]]),
                        new Column('list', Type::getType('json'), ['notnull' => true, 'columnDefinition' => 'integer[]']),
                        new Column('map', Type::getType('json'), ['notnull' => true, 'comment' => 'test comment!']),
                        new Column('date', Type::getType('date_immutable'), ['notnull' => false]),
                    ],
                    [
                        new Index('pk_test', ['int', 'str'], true, true),
                        new Index('idx_date', ['date'], false, false),
                        new Index('idx_str_unique', ['str_unique'], true, false),
                    ]
                )
            )
        );
    }
}
