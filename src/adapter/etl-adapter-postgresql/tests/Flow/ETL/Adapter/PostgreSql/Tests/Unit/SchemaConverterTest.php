<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\PostgreSqlMetadata;
use Flow\ETL\Adapter\PostgreSql\SchemaConverter;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\Constraint\PrimaryKey;
use Flow\PostgreSql\Schema\Constraint\UniqueConstraint;
use Flow\PostgreSql\Schema\IdentityGeneration;
use Flow\PostgreSql\Schema\Index;
use Flow\PostgreSql\Schema\PartitionStrategy;
use Flow\PostgreSql\Schema\Table;
use Flow\PostgreSql\Schema\TriggerEvent;
use Flow\PostgreSql\Schema\TriggerTiming;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\PostgreSql\DSL\schema_check;
use function Flow\PostgreSql\DSL\schema_exclude;
use function Flow\PostgreSql\DSL\schema_foreign_key;
use function Flow\PostgreSql\DSL\schema_table_options;
use function Flow\PostgreSql\DSL\schema_trigger;

final class SchemaConverterTest extends TestCase
{
    public function test_composite_primary_key(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(
                int_schema('order_id', metadata: PostgreSqlMetadata::primaryKey('pk_order_item')),
                int_schema('item_id', metadata: PostgreSqlMetadata::primaryKey('pk_order_item')),
                str_schema('note'),
            ),
            'order_items',
        );

        static::assertInstanceOf(PrimaryKey::class, $table->primaryKey);
        static::assertSame(['order_id', 'item_id'], $table->primaryKey->columns);
        static::assertSame('pk_order_item', $table->primaryKey->name);
    }

    public function test_default_value(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(bool_schema('active', metadata: PostgreSqlMetadata::default(true))),
            'flags',
        );

        static::assertSame('true', $table->column('active')->default?->literal);
    }

    public function test_empty_schema_throws(): void
    {
        $this->expectException(RuntimeException::class);

        (new SchemaConverter())->toPostgreSqlTable(schema(), 'empty');
    }

    public function test_explicit_type_override(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(str_schema('payload', metadata: PostgreSqlMetadata::type('jsonb'))),
            'events',
        );

        static::assertTrue($table->column('payload')->type->isEqual(ColumnType::jsonb()));
    }

    public function test_forward_maps_scalar_types(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(
                int_schema('id'),
                str_schema('name'),
                float_schema('price'),
                bool_schema('active'),
                datetime_schema('created_at'),
                uuid_schema('uuid'),
                json_schema('payload'),
                xml_schema('document'),
            ),
            'records',
        );

        static::assertTrue($table->column('id')->type->isEqual(ColumnType::bigint()));
        static::assertTrue($table->column('name')->type->isEqual(ColumnType::text()));
        static::assertTrue($table->column('price')->type->isEqual(ColumnType::doublePrecision()));
        static::assertTrue($table->column('active')->type->isEqual(ColumnType::boolean()));
        static::assertTrue($table->column('created_at')->type->isEqual(ColumnType::timestamp()));
        static::assertTrue($table->column('uuid')->type->isEqual(ColumnType::uuid()));
        static::assertTrue($table->column('payload')->type->isEqual(ColumnType::jsonb()));
        static::assertTrue($table->column('document')->type->isEqual(ColumnType::xml()));
    }

    public function test_generated_column(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(int_schema('total', metadata: PostgreSqlMetadata::generated('price * quantity'))),
            'line_items',
        );

        static::assertTrue($table->column('total')->isGenerated);
        static::assertNotNull($table->column('total')->generationExpression);
    }

    public function test_identity_column(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(int_schema('id', metadata: PostgreSqlMetadata::identity(IdentityGeneration::BY_DEFAULT))),
            'users',
        );

        static::assertTrue($table->column('id')->isIdentity);
        static::assertSame(IdentityGeneration::BY_DEFAULT, $table->column('id')->identityGeneration);
    }

    public function test_length_produces_varchar(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(str_schema('name', metadata: PostgreSqlMetadata::length(120))),
            'users',
        );

        static::assertTrue($table->column('name')->type->isEqual(ColumnType::varchar(120)));
    }

    public function test_named_index(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(str_schema('email', metadata: PostgreSqlMetadata::index('idx_email'))),
            'users',
        );

        static::assertCount(1, $table->indexes);
        static::assertSame('idx_email', $table->indexes[0]->name);
        static::assertSame(['email'], $table->indexes[0]->columns);
        static::assertFalse($table->indexes[0]->unique);
    }

    public function test_index_explicit_position_overrides_schema_order(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(
                int_schema('id', metadata: PostgreSqlMetadata::index('idx', 2)),
                datetime_schema('created_at', metadata: PostgreSqlMetadata::index('idx', 1)),
            ),
            'orders',
        );

        static::assertCount(1, $table->indexes);
        static::assertSame('idx', $table->indexes[0]->name);
        static::assertSame(['created_at', 'id'], $table->indexes[0]->columns);
    }

    public function test_composite_index_without_positions_preserves_schema_order(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(
                int_schema('id', metadata: PostgreSqlMetadata::index('idx')),
                datetime_schema('created_at', metadata: PostgreSqlMetadata::index('idx')),
            ),
            'orders',
        );

        static::assertSame(['id', 'created_at'], $table->indexes[0]->columns);
    }

    public function test_composite_unique_with_positions_is_ordered(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(
                int_schema('warehouse', metadata: PostgreSqlMetadata::indexUnique('uq_stock', 2)),
                int_schema('product_id', metadata: PostgreSqlMetadata::indexUnique('uq_stock', 1)),
            ),
            'stock',
        );

        static::assertCount(1, $table->uniqueConstraints);
        static::assertSame(['product_id', 'warehouse'], $table->uniqueConstraints[0]->columns);
        static::assertSame('uq_stock', $table->uniqueConstraints[0]->name);
    }

    public function test_column_can_belong_to_multiple_indexes(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(
                int_schema(
                    'id',
                    metadata: PostgreSqlMetadata::index('idx_a', 1)->merge(PostgreSqlMetadata::index('idx_b', 2)),
                ),
                datetime_schema('created_at', metadata: PostgreSqlMetadata::index('idx_b', 1)),
            ),
            'orders',
        );

        $byName = [];

        foreach ($table->indexes as $index) {
            $byName[$index->name] = $index->columns;
        }

        static::assertArrayHasKey('idx_a', $byName);
        static::assertArrayHasKey('idx_b', $byName);
        static::assertSame(['id'], $byName['idx_a']);
        static::assertSame(['created_at', 'id'], $byName['idx_b']);
    }

    public function test_reverse_preserves_index_column_order(): void
    {
        $table = new Table(
            schema: 'public',
            name: 'orders',
            columns: [
                Column::create('id', ColumnType::bigint(), nullable: false),
                Column::create('created_at', ColumnType::timestamptz()),
            ],
            indexes: [
                new Index(name: 'idx', columns: ['created_at', 'id']),
            ],
        );

        $converter = new SchemaConverter();
        $roundTripped = $converter->toPostgreSqlTable($converter->toFlowSchema($table), 'orders');

        static::assertCount(1, $roundTripped->indexes);
        static::assertSame(['created_at', 'id'], $roundTripped->indexes[0]->columns);
    }

    public function test_nullability_from_definition(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(str_schema('required', nullable: false), str_schema('optional', nullable: true)),
            'records',
        );

        static::assertFalse($table->column('required')->nullable);
        static::assertTrue($table->column('optional')->nullable);
    }

    public function test_precision_and_scale_produce_numeric(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(float_schema(
                'amount',
                metadata: PostgreSqlMetadata::precision(10)->merge(PostgreSqlMetadata::scale(2)),
            )),
            'invoices',
        );

        static::assertTrue($table->column('amount')->type->isEqual(ColumnType::numeric(10, 2)));
    }

    public function test_primary_key_forces_not_null(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(int_schema('id', nullable: true, metadata: PostgreSqlMetadata::primaryKey())),
            'users',
        );

        static::assertFalse($table->column('id')->nullable);
    }

    public function test_reverse_maps_columns_to_flow_definitions(): void
    {
        $table = new Table(schema: 'public', name: 'records', columns: [
            Column::create('id', ColumnType::bigint(), nullable: false),
            Column::create('name', ColumnType::varchar(120)),
            Column::create('price', ColumnType::doublePrecision()),
            Column::create('active', ColumnType::boolean()),
            Column::create('created_at', ColumnType::timestamptz()),
            Column::create('uuid', ColumnType::uuid()),
            Column::create('payload', ColumnType::jsonb()),
            Column::create('document', ColumnType::xml()),
        ]);

        $flowSchema = (new SchemaConverter())->toFlowSchema($table);

        static::assertInstanceOf(IntegerType::class, $flowSchema->get('id')->type());
        static::assertInstanceOf(StringType::class, $flowSchema->get('name')->type());
        static::assertInstanceOf(FloatType::class, $flowSchema->get('price')->type());
        static::assertInstanceOf(BooleanType::class, $flowSchema->get('active')->type());
        static::assertInstanceOf(DateTimeType::class, $flowSchema->get('created_at')->type());
        static::assertInstanceOf(UuidType::class, $flowSchema->get('uuid')->type());
        static::assertInstanceOf(JsonType::class, $flowSchema->get('payload')->type());
        static::assertInstanceOf(XMLType::class, $flowSchema->get('document')->type());
    }

    public function test_reverse_preserves_length_metadata(): void
    {
        $table = new Table(schema: 'public', name: 'users', columns: [Column::create(
            'name',
            ColumnType::varchar(120),
        )]);

        $definition = (new SchemaConverter())->toFlowSchema($table)->get('name');

        static::assertTrue($definition->metadata()->has(PostgreSqlMetadata::LENGTH->value));
        static::assertSame(120, $definition->metadata()->get(PostgreSqlMetadata::LENGTH->value));
    }

    public function test_reverse_primary_key_is_not_nullable(): void
    {
        $table = new Table(
            schema: 'public',
            name: 'users',
            columns: [Column::create('id', ColumnType::bigint(), nullable: true)],
            primaryKey: new PrimaryKey(['id'], 'pk_users'),
        );

        $definition = (new SchemaConverter())->toFlowSchema($table)->get('id');

        static::assertFalse($definition->isNullable());
        static::assertSame('pk_users', $definition->metadata()->get(PostgreSqlMetadata::PRIMARY_KEY->value));
    }

    public function test_round_trip_preserves_structure(): void
    {
        $original = schema(
            int_schema('id', metadata: PostgreSqlMetadata::primaryKey('pk_users')),
            str_schema('email', metadata: PostgreSqlMetadata::indexUnique('uq_email')),
            json_schema('payload'),
        );

        $converter = new SchemaConverter();
        $roundTripped = $converter->toFlowSchema($converter->toPostgreSqlTable($original, 'users'));

        static::assertInstanceOf(IntegerType::class, $roundTripped->get('id')->type());
        static::assertInstanceOf(StringType::class, $roundTripped->get('email')->type());
        static::assertInstanceOf(JsonType::class, $roundTripped->get('payload')->type());
        static::assertFalse($roundTripped->get('id')->isNullable());
    }

    public function test_two_primary_keys_throw(): void
    {
        $this->expectException(RuntimeException::class);

        (new SchemaConverter())->toPostgreSqlTable(
            schema(
                int_schema('a', metadata: PostgreSqlMetadata::primaryKey('pk_a')),
                int_schema('b', metadata: PostgreSqlMetadata::primaryKey('pk_b')),
            ),
            'records',
        );
    }

    public function test_unique_constraint(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(str_schema('email', metadata: PostgreSqlMetadata::indexUnique('uq_email'))),
            'users',
        );

        static::assertCount(1, $table->uniqueConstraints);
        static::assertInstanceOf(UniqueConstraint::class, $table->uniqueConstraints[0]);
        static::assertSame(['email'], $table->uniqueConstraints[0]->columns);
        static::assertSame('uq_email', $table->uniqueConstraints[0]->name);
    }

    public function test_no_options_keeps_defaults(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(schema(int_schema('id')), 'events');

        static::assertFalse($table->unlogged);
        static::assertNull($table->tablespace);
        static::assertSame([], $table->inherits);
        static::assertSame([], $table->foreignKeys);
        static::assertSame([], $table->checkConstraints);
        static::assertSame([], $table->excludeConstraints);
        static::assertSame([], $table->triggers);
        static::assertNull($table->partitionStrategy);
    }

    public function test_options_collections_thread_into_table(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(int_schema('id'), int_schema('user_id')),
            'orders',
            'public',
            schema_table_options(
                foreignKeys: [schema_foreign_key(['user_id'], 'users', ['id'])],
                checkConstraints: [schema_check('id > 0')],
                excludeConstraints: [schema_exclude('USING gist (tsrange WITH &&)')],
                triggers: [schema_trigger('trg', 'orders', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'fn')],
            ),
        );

        static::assertCount(1, $table->foreignKeys);
        static::assertSame('users', $table->foreignKeys[0]->referenceTable);
        static::assertCount(1, $table->checkConstraints);
        static::assertCount(1, $table->excludeConstraints);
        static::assertCount(1, $table->triggers);
        static::assertSame('trg', $table->triggers[0]->name);
    }

    public function test_options_thread_into_table(): void
    {
        $table = (new SchemaConverter())->toPostgreSqlTable(
            schema(int_schema('id')),
            'events',
            'public',
            schema_table_options(
                unlogged: true,
                partitionStrategy: PartitionStrategy::RANGE,
                partitionColumns: ['id'],
                inherits: ['parent'],
                tablespace: 'fast_storage',
            ),
        );

        static::assertTrue($table->unlogged);
        static::assertSame(PartitionStrategy::RANGE, $table->partitionStrategy);
        static::assertSame(['id'], $table->partitionColumns);
        static::assertSame(['parent'], $table->inherits);
        static::assertSame('fast_storage', $table->tablespace);
    }
}
