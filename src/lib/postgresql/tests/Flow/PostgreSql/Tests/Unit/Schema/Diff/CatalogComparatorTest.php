<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\Diff\StrictRenameStrategy;
use Flow\PostgreSql\Schema\FunctionVolatility;
use Flow\PostgreSql\Schema\Trigger;
use Flow\PostgreSql\Schema\TriggerEvent;
use Flow\PostgreSql\Schema\TriggerTiming;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\PostgreSql\DSL\and_;
use function Flow\PostgreSql\DSL\catalog_comparator;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_check;
use function Flow\PostgreSql\DSL\schema_column_bigint;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_column_uuid;
use function Flow\PostgreSql\DSL\schema_domain;
use function Flow\PostgreSql\DSL\schema_exclude;
use function Flow\PostgreSql\DSL\schema_extension;
use function Flow\PostgreSql\DSL\schema_foreign_key;
use function Flow\PostgreSql\DSL\schema_function;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_materialized_view;
use function Flow\PostgreSql\DSL\schema_primary_key;
use function Flow\PostgreSql\DSL\schema_procedure;
use function Flow\PostgreSql\DSL\schema_sequence;
use function Flow\PostgreSql\DSL\schema_table;
use function Flow\PostgreSql\DSL\schema_unique;
use function Flow\PostgreSql\DSL\schema_view;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function ksort;

final class CatalogComparatorTest extends TestCase
{
    public function test_added_column_detected(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('email'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedColumns);
        static::assertSame('email', $tableDiff->addedColumns[0]->name);
    }

    public function test_added_index_detected(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false), schema_column_text('email')]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    indexes: [
                        schema_index('idx_email', ['email']),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedIndexes);
        static::assertSame('idx_email', $tableDiff->addedIndexes[0]->name);
    }

    public function test_added_schema_detected(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([schema('public'), schema('audit')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->addedSchemas);
        static::assertSame('audit', $diff->addedSchemas[0]->name);
        static::assertCount(0, $diff->removedSchemas);
    }

    public function test_added_table_detected(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas);
        static::assertCount(1, $diff->modifiedSchemas[0]->addedTables);
        static::assertSame('users', $diff->modifiedSchemas[0]->addedTables[0]->name);
    }

    public function test_ambiguous_column_rename_resolved_by_similarity(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('first_name'),
                    schema_column_text('last_name'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('full_name'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(0, $tableDiff->addedColumns);
        static::assertCount(1, $tableDiff->removedColumns);
        static::assertSame('first_name', $tableDiff->removedColumns[0]->name);
        static::assertCount(1, $tableDiff->modifiedColumns);
        static::assertTrue($tableDiff->modifiedColumns[0]->hasNameChanged());
        static::assertSame('last_name', $tableDiff->modifiedColumns[0]->source->name);
        static::assertSame('full_name', $tableDiff->modifiedColumns[0]->target->name);
    }

    public function test_ambiguous_index_rename_resolved_by_similarity(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    indexes: [
                        schema_index('idx_email_a', ['email']),
                        schema_index('idx_email_b', ['email']),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    indexes: [
                        schema_index('idx_email_c', ['email']),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(0, $tableDiff->addedIndexes);
        static::assertCount(1, $tableDiff->removedIndexes);
        static::assertCount(1, $tableDiff->renamedIndexes);
    }

    public function test_both_empty_catalogs_produce_empty_diff(): void
    {
        $source = new Catalog([]);
        $target = new Catalog([]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertTrue($diff->isEmpty());
    }

    public function test_both_empty_schemas_produce_empty_diff(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([schema('public')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertTrue($diff->isEmpty());
    }

    public function test_check_constraint_added(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('age')]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('age')],
                    checkConstraints: [
                        schema_check('age > 0', 'chk_age_positive'),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedCheckConstraints);
        static::assertSame('age > 0', $tableDiff->addedCheckConstraints[0]->expression);
    }

    public function test_check_constraint_modified_treated_as_drop_and_add(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('age')],
                    checkConstraints: [
                        schema_check('age > 0', 'chk_age'),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('age')],
                    checkConstraints: [
                        schema_check('age > 18', 'chk_age'),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedCheckConstraints);
        static::assertSame('age > 18', $tableDiff->addedCheckConstraints[0]->expression);
        static::assertCount(1, $tableDiff->removedCheckConstraints);
        static::assertSame('age > 0', $tableDiff->removedCheckConstraints[0]->expression);
    }

    public function test_check_constraint_no_inherit_change_detected(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('age')],
                    checkConstraints: [
                        schema_check('age > 0', 'chk_age', false),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('age')],
                    checkConstraints: [
                        schema_check('age > 0', 'chk_age', true),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedCheckConstraints);
        static::assertCount(1, $tableDiff->removedCheckConstraints);
    }

    public function test_check_constraint_removed(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('age')],
                    checkConstraints: [
                        schema_check('age > 0', 'chk_age_positive'),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('age')]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->removedCheckConstraints);
        static::assertSame('age > 0', $tableDiff->removedCheckConstraints[0]->expression);
    }

    public function test_check_constraint_without_name_uses_expression_for_matching(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('age')],
                    checkConstraints: [
                        schema_check('age > 0'),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_column_add_and_remove_not_confused_as_rename(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('foo'),
                    schema_column_text('bar'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('xyz_completely_different_one'),
                    schema_column_text('xyz_completely_different_two'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(2, $tableDiff->addedColumns);
        static::assertCount(2, $tableDiff->removedColumns);
        static::assertCount(0, $tableDiff->modifiedColumns);
    }

    public function test_column_rename_detected(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('name'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('full_name'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(0, $tableDiff->addedColumns);
        static::assertCount(0, $tableDiff->removedColumns);
        static::assertCount(1, $tableDiff->modifiedColumns);
        static::assertTrue($tableDiff->modifiedColumns[0]->hasNameChanged());
        static::assertSame('name', $tableDiff->modifiedColumns[0]->source->name);
        static::assertSame('full_name', $tableDiff->modifiedColumns[0]->target->name);
    }

    public function test_column_rename_detected_with_ambiguous_candidates(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('orders', [
                    schema_column_integer('id', false),
                    schema_column_bigint('net_commission_cents'),
                    schema_column_bigint('retail_agency_commission_cents'),
                    schema_column_bigint('total_commission_cents'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('orders', [
                    schema_column_integer('id', false),
                    schema_column_bigint('net_commission'),
                    schema_column_bigint('retail_agency_commission'),
                    schema_column_bigint('total_commission'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(0, $tableDiff->addedColumns);
        static::assertCount(0, $tableDiff->removedColumns);
        static::assertCount(3, $tableDiff->modifiedColumns);

        $renames = [];

        foreach ($tableDiff->modifiedColumns as $col) {
            static::assertTrue($col->hasNameChanged());
            $renames[$col->source->name] = $col->target->name;
        }

        ksort($renames);
        static::assertSame(
            [
                'net_commission_cents' => 'net_commission',
                'retail_agency_commission_cents' => 'retail_agency_commission',
                'total_commission_cents' => 'total_commission',
            ],
            $renames,
        );
    }

    public function test_column_rename_not_detected_when_type_changes(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('name'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_integer('full_name'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedColumns);
        static::assertSame('full_name', $tableDiff->addedColumns[0]->name);
        static::assertCount(1, $tableDiff->removedColumns);
        static::assertSame('name', $tableDiff->removedColumns[0]->name);
        static::assertCount(0, $tableDiff->modifiedColumns);
    }

    public function test_column_rename_with_mixed_types(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('orders', [
                    schema_column_integer('id', false),
                    schema_column_bigint('amount_cents'),
                    schema_column_bigint('total_cents'),
                    schema_column_text('description_old'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('orders', [
                    schema_column_integer('id', false),
                    schema_column_bigint('amount'),
                    schema_column_bigint('total'),
                    schema_column_text('description'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(0, $tableDiff->addedColumns);
        static::assertCount(0, $tableDiff->removedColumns);
        static::assertCount(3, $tableDiff->modifiedColumns);

        $renames = [];

        foreach ($tableDiff->modifiedColumns as $col) {
            static::assertTrue($col->hasNameChanged());
            $renames[$col->source->name] = $col->target->name;
        }

        ksort($renames);
        static::assertSame(
            [
                'amount_cents' => 'amount',
                'description_old' => 'description',
                'total_cents' => 'total',
            ],
            $renames,
        );
    }

    public function test_column_rename_with_strict_rename_strategy(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('orders', [
                    schema_column_integer('id', false),
                    schema_column_bigint('net_commission_cents'),
                    schema_column_bigint('retail_agency_commission_cents'),
                    schema_column_bigint('total_commission_cents'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('orders', [
                    schema_column_integer('id', false),
                    schema_column_bigint('net_commission'),
                    schema_column_bigint('retail_agency_commission'),
                    schema_column_bigint('total_commission'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator(new StrictRenameStrategy())->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(3, $tableDiff->addedColumns);
        static::assertCount(3, $tableDiff->removedColumns);
        static::assertCount(0, $tableDiff->modifiedColumns);
    }

    public function test_domain_added(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([
            schema('public', domains: [
                schema_domain('email', ColumnType::text()),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->addedDomains);
        static::assertSame('email', $diff->modifiedSchemas[0]->addedDomains[0]->name);
    }

    public function test_domain_base_type_change_detected(): void
    {
        $source = new Catalog([
            schema('public', domains: [
                schema_domain('positive_int', ColumnType::integer()),
            ]),
        ]);
        $target = new Catalog([
            schema('public', domains: [
                schema_domain('positive_int', ColumnType::bigint()),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedDomains);
        static::assertTrue($diff->modifiedSchemas[0]->modifiedDomains[0]->hasBaseTypeChanged());
    }

    public function test_domain_check_constraint_change_detected(): void
    {
        $source = new Catalog([
            schema('public', domains: [
                schema_domain('email', ColumnType::text(), checkConstraints: [
                    schema_check('VALUE ~ \'@\'', 'chk_email'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', domains: [
                schema_domain('email', ColumnType::text(), checkConstraints: [
                    schema_check('VALUE ~ \'@.+\\.\'', 'chk_email'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedDomains);
        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedDomains[0]->addedCheckConstraints);
        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedDomains[0]->removedCheckConstraints);
    }

    public function test_domain_default_change_detected(): void
    {
        $source = new Catalog([
            schema('public', domains: [
                schema_domain('email', ColumnType::text(), default: 'old'),
            ]),
        ]);
        $target = new Catalog([
            schema('public', domains: [
                schema_domain('email', ColumnType::text(), default: 'new'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedDomains);
        static::assertTrue($diff->modifiedSchemas[0]->modifiedDomains[0]->hasDefaultChanged());
    }

    public function test_domain_no_change_when_identical(): void
    {
        $source = new Catalog([
            schema('public', domains: [
                schema_domain('email', ColumnType::text(), nullable: false, default: 'x', checkConstraints: [
                    schema_check('VALUE ~ \'@\'', 'chk_email'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_domain_nullable_change_detected(): void
    {
        $source = new Catalog([
            schema('public', domains: [
                schema_domain('email', ColumnType::text(), nullable: true),
            ]),
        ]);
        $target = new Catalog([
            schema('public', domains: [
                schema_domain('email', ColumnType::text(), nullable: false),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedDomains);
        static::assertTrue($diff->modifiedSchemas[0]->modifiedDomains[0]->hasNullableChanged());
    }

    public function test_domain_removed(): void
    {
        $source = new Catalog([
            schema('public', domains: [
                schema_domain('email', ColumnType::text()),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->removedDomains);
    }

    public function test_exclude_constraint_added(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('events', [
                    schema_column_integer('id', false),
                    schema_column_text('room'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'events',
                    [
                        schema_column_integer('id', false),
                        schema_column_text('room'),
                    ],
                    excludeConstraints: [
                        schema_exclude('USING gist (room WITH =)', 'excl_room'),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedExcludeConstraints);
        static::assertSame('USING gist (room WITH =)', $tableDiff->addedExcludeConstraints[0]->definition);
    }

    public function test_exclude_constraint_modified_treated_as_drop_and_add(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'events',
                    [
                        schema_column_integer('id', false),
                        schema_column_text('room'),
                    ],
                    excludeConstraints: [
                        schema_exclude('USING gist (room WITH =)', 'excl_room'),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'events',
                    [
                        schema_column_integer('id', false),
                        schema_column_text('room'),
                    ],
                    excludeConstraints: [
                        schema_exclude('USING gist (room WITH &&)', 'excl_room'),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedExcludeConstraints);
        static::assertCount(1, $tableDiff->removedExcludeConstraints);
    }

    public function test_exclude_constraint_removed(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'events',
                    [
                        schema_column_integer('id', false),
                        schema_column_text('room'),
                    ],
                    excludeConstraints: [
                        schema_exclude('USING gist (room WITH =)', 'excl_room'),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('events', [
                    schema_column_integer('id', false),
                    schema_column_text('room'),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->removedExcludeConstraints);
    }

    public function test_exclude_constraint_without_name_uses_definition_for_matching(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'events',
                    [
                        schema_column_integer('id', false),
                        schema_column_text('room'),
                    ],
                    excludeConstraints: [
                        schema_exclude('USING gist (room WITH =)'),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_extension_added(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([
            schema('public', extensions: [
                schema_extension('uuid-ossp', '1.0'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->addedExtensions);
        static::assertSame('uuid-ossp', $diff->modifiedSchemas[0]->addedExtensions[0]->name);
    }

    public function test_extension_no_change_when_identical(): void
    {
        $source = new Catalog([
            schema('public', extensions: [
                schema_extension('uuid-ossp', '1.0'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_extension_removed(): void
    {
        $source = new Catalog([
            schema('public', extensions: [
                schema_extension('uuid-ossp', '1.0'),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->removedExtensions);
    }

    public function test_extension_version_change_detected(): void
    {
        $source = new Catalog([
            schema('public', extensions: [
                schema_extension('uuid-ossp', '1.0'),
            ]),
        ]);
        $target = new Catalog([
            schema('public', extensions: [
                schema_extension('uuid-ossp', '1.1'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedExtensions);
        static::assertTrue($diff->modifiedSchemas[0]->modifiedExtensions[0]->hasVersionChanged());
    }

    public function test_foreign_key_added(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('orders', [
                    schema_column_integer('id', false),
                    schema_column_integer('user_id', false),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'users', ['id']),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedForeignKeys);
        static::assertSame(['user_id'], $tableDiff->addedForeignKeys[0]->columns);
    }

    public function test_foreign_key_modified_columns_change(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_user'),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'users', ['uuid'], name: 'fk_user'),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedForeignKeys);
        static::assertCount(1, $tableDiff->removedForeignKeys);
    }

    public function test_foreign_key_modified_deferrable_change(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_user', deferrable: false),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_user', deferrable: true),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedForeignKeys);
        static::assertCount(1, $tableDiff->removedForeignKeys);
    }

    public function test_foreign_key_modified_initially_deferred_change(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(
                            ['user_id'],
                            'users',
                            ['id'],
                            name: 'fk_user',
                            deferrable: true,
                            initiallyDeferred: false,
                        ),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(
                            ['user_id'],
                            'users',
                            ['id'],
                            name: 'fk_user',
                            deferrable: true,
                            initiallyDeferred: true,
                        ),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedForeignKeys);
        static::assertCount(1, $tableDiff->removedForeignKeys);
    }

    public function test_foreign_key_modified_on_delete_action_change(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(
                            ['user_id'],
                            'users',
                            ['id'],
                            name: 'fk_user',
                            onDelete: ReferentialAction::NO_ACTION,
                        ),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(
                            ['user_id'],
                            'users',
                            ['id'],
                            name: 'fk_user',
                            onDelete: ReferentialAction::CASCADE,
                        ),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedForeignKeys);
        static::assertCount(1, $tableDiff->removedForeignKeys);
    }

    public function test_foreign_key_modified_on_update_action_change(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(
                            ['user_id'],
                            'users',
                            ['id'],
                            name: 'fk_user',
                            onUpdate: ReferentialAction::NO_ACTION,
                        ),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(
                            ['user_id'],
                            'users',
                            ['id'],
                            name: 'fk_user',
                            onUpdate: ReferentialAction::CASCADE,
                        ),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedForeignKeys);
        static::assertCount(1, $tableDiff->removedForeignKeys);
    }

    public function test_foreign_key_modified_reference_schema_change(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_user', referenceSchema: 'public'),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_user', referenceSchema: 'auth'),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedForeignKeys);
        static::assertCount(1, $tableDiff->removedForeignKeys);
    }

    public function test_foreign_key_modified_reference_table_change(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_user'),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'accounts', ['id'], name: 'fk_user'),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedForeignKeys);
        static::assertCount(1, $tableDiff->removedForeignKeys);
    }

    public function test_foreign_key_removed(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'users', ['id']),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('orders', [
                    schema_column_integer('id', false),
                    schema_column_integer('user_id', false),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->removedForeignKeys);
    }

    public function test_foreign_key_without_name_uses_identity_for_matching(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'orders',
                    [
                        schema_column_integer('id', false),
                        schema_column_integer('user_id', false),
                    ],
                    foreignKeys: [
                        schema_foreign_key(['user_id'], 'users', ['id']),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_function_added(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([
            schema('public', functions: [
                schema_function(
                    'get_user',
                    'text',
                    ['integer'],
                    'sql',
                    select(col('name'))
                        ->from(table('users'))
                        ->where(eq(col('id'), literal(1)))
                        ->toSql(),
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->addedFunctions);
        static::assertSame('get_user', $diff->modifiedSchemas[0]->addedFunctions[0]->name);
    }

    public function test_function_modified_argument_types_change(): void
    {
        $source = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer']),
            ]),
        ]);
        $target = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['text']),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedFunctions);
    }

    public function test_function_modified_definition_change(): void
    {
        $source = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer'], definition: select(literal(1))->toSql()),
            ]),
        ]);
        $target = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer'], definition: select(literal(2))->toSql()),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedFunctions);
    }

    public function test_function_modified_language_change(): void
    {
        $source = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer'], 'sql'),
            ]),
        ]);
        $target = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer'], 'plpgsql'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedFunctions);
    }

    public function test_function_modified_return_type_change(): void
    {
        $source = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer']),
            ]),
        ]);
        $target = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'integer', ['integer']),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedFunctions);
    }

    public function test_function_modified_strict_change(): void
    {
        $source = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer'], isStrict: false),
            ]),
        ]);
        $target = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer'], isStrict: true),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedFunctions);
    }

    public function test_function_modified_volatility_change(): void
    {
        $source = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer'], volatility: FunctionVolatility::VOLATILE),
            ]),
        ]);
        $target = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer'], volatility: FunctionVolatility::IMMUTABLE),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedFunctions);
    }

    public function test_function_no_change_when_identical(): void
    {
        $source = new Catalog([
            schema('public', functions: [
                schema_function(
                    'get_user',
                    'text',
                    ['integer'],
                    'sql',
                    select(literal(1))->toSql(),
                    true,
                    FunctionVolatility::STABLE,
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_function_removed(): void
    {
        $source = new Catalog([
            schema('public', functions: [
                schema_function('get_user', 'text', ['integer']),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->removedFunctions);
    }

    public function test_identical_catalogs_produce_empty_diff(): void
    {
        $catalog = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [
                        schema_column_integer('id', false),
                        schema_column_text('name'),
                    ],
                    primaryKey: schema_primary_key(['id']),
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($catalog, $catalog);

        static::assertTrue($diff->isEmpty());
    }

    public function test_identical_table_not_in_modified_tables(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [
                        schema_column_integer('id', false),
                        schema_column_text('name'),
                    ],
                    primaryKey: schema_primary_key(['id']),
                    indexes: [
                        schema_index('idx_name', ['name']),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
        static::assertCount(0, $diff->modifiedSchemas);
    }

    public function test_index_rename_detected(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    indexes: [
                        schema_index('idx_email_old', ['email']),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    indexes: [
                        schema_index('idx_email_new', ['email']),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(0, $tableDiff->addedIndexes);
        static::assertCount(0, $tableDiff->removedIndexes);
        static::assertCount(1, $tableDiff->renamedIndexes);
        static::assertArrayHasKey('idx_email_old', $tableDiff->renamedIndexes);
        static::assertSame('idx_email_new', $tableDiff->renamedIndexes['idx_email_old']->name);
    }

    public function test_index_with_same_name_but_different_structure(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email'), schema_column_text('name')],
                    indexes: [
                        schema_index('idx_users', ['email']),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email'), schema_column_text('name')],
                    indexes: [
                        schema_index('idx_users', ['name']),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedIndexes);
        static::assertSame('idx_users', $tableDiff->addedIndexes[0]->name);
        static::assertCount(1, $tableDiff->removedIndexes);
        static::assertSame('idx_users', $tableDiff->removedIndexes[0]->name);
    }

    public function test_materialized_view_added(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([
            schema('public', materializedViews: [
                schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->addedMaterializedViews);
        static::assertSame('mv_users', $diff->modifiedSchemas[0]->addedMaterializedViews[0]->name);
    }

    public function test_materialized_view_definition_change_detected(): void
    {
        $source = new Catalog([
            schema('public', materializedViews: [
                schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            ]),
        ]);
        $target = new Catalog([
            schema('public', materializedViews: [
                schema_materialized_view(
                    'mv_users',
                    select(star())
                        ->from(table('users'))
                        ->where(eq(col('active'), literal(true)))
                        ->toSql(),
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedMaterializedViews);
        static::assertTrue($diff->modifiedSchemas[0]->modifiedMaterializedViews[0]->hasDefinitionChanged());
    }

    public function test_materialized_view_index_change_detected(): void
    {
        $source = new Catalog([
            schema('public', materializedViews: [
                schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            ]),
        ]);
        $target = new Catalog([
            schema('public', materializedViews: [
                schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql(), indexes: [
                    schema_index('idx_mv_users', ['id']),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedMaterializedViews);
        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedMaterializedViews[0]->addedIndexes);
    }

    public function test_materialized_view_no_change_when_identical(): void
    {
        $source = new Catalog([
            schema('public', materializedViews: [
                schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_materialized_view_removed(): void
    {
        $source = new Catalog([
            schema('public', materializedViews: [
                schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->removedMaterializedViews);
    }

    public function test_modified_column_detected(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('name'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('name', false),
                ]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->modifiedColumns);
        static::assertSame('name', $tableDiff->modifiedColumns[0]->source->name);
        static::assertTrue($tableDiff->modifiedColumns[0]->hasNullableChanged());
        static::assertFalse($tableDiff->modifiedColumns[0]->hasNameChanged());
    }

    public function test_primary_key_added(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], primaryKey: schema_primary_key(['id'])),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        $addedPrimaryKey = $tableDiff->addedPrimaryKey;
        static::assertNotNull($addedPrimaryKey);
        static::assertNull($tableDiff->removedPrimaryKey);
        static::assertSame(['id'], $addedPrimaryKey->columns);
    }

    public function test_primary_key_changed(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [
                        schema_column_integer('id', false),
                        schema_column_uuid('uuid', false),
                    ],
                    primaryKey: schema_primary_key(['id']),
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [
                        schema_column_integer('id', false),
                        schema_column_uuid('uuid', false),
                    ],
                    primaryKey: schema_primary_key(['uuid']),
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        $addedPrimaryKey = $tableDiff->addedPrimaryKey;
        $removedPrimaryKey = $tableDiff->removedPrimaryKey;
        static::assertNotNull($addedPrimaryKey);
        static::assertNotNull($removedPrimaryKey);
        static::assertSame(['uuid'], $addedPrimaryKey->columns);
        static::assertSame(['id'], $removedPrimaryKey->columns);
    }

    public function test_primary_key_removed(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], primaryKey: schema_primary_key(['id'])),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertNull($tableDiff->addedPrimaryKey);
        static::assertNotNull($tableDiff->removedPrimaryKey);
    }

    public function test_procedure_added(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([
            schema('public', procedures: [
                schema_procedure('cleanup', ['integer'], 'sql', 'DELETE FROM logs WHERE id = $1'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->addedProcedures);
        static::assertSame('cleanup', $diff->modifiedSchemas[0]->addedProcedures[0]->name);
    }

    public function test_procedure_modified_argument_types_change(): void
    {
        $source = new Catalog([
            schema('public', procedures: [
                schema_procedure('cleanup', ['integer']),
            ]),
        ]);
        $target = new Catalog([
            schema('public', procedures: [
                schema_procedure('cleanup', ['text']),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedProcedures);
    }

    public function test_procedure_modified_definition_change(): void
    {
        $source = new Catalog([
            schema('public', procedures: [
                schema_procedure('cleanup', ['integer'], definition: 'DELETE FROM logs WHERE id = $1'),
            ]),
        ]);
        $target = new Catalog([
            schema('public', procedures: [
                schema_procedure('cleanup', ['integer'], definition: 'DELETE FROM logs'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedProcedures);
    }

    public function test_procedure_modified_language_change(): void
    {
        $source = new Catalog([
            schema('public', procedures: [
                schema_procedure('cleanup', ['integer'], 'sql'),
            ]),
        ]);
        $target = new Catalog([
            schema('public', procedures: [
                schema_procedure('cleanup', ['integer'], 'plpgsql'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedProcedures);
    }

    public function test_procedure_no_change_when_identical(): void
    {
        $source = new Catalog([
            schema('public', procedures: [
                schema_procedure('cleanup', ['integer'], 'sql', 'DELETE FROM logs'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_procedure_removed(): void
    {
        $source = new Catalog([
            schema('public', procedures: [
                schema_procedure('cleanup', ['integer']),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->removedProcedures);
    }

    public function test_removed_column_detected(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [
                    schema_column_integer('id', false),
                    schema_column_text('email'),
                ]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->removedColumns);
        static::assertSame('email', $tableDiff->removedColumns[0]->name);
    }

    public function test_removed_index_detected(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    indexes: [
                        schema_index('idx_email', ['email']),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false), schema_column_text('email')]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->removedIndexes);
        static::assertSame('idx_email', $tableDiff->removedIndexes[0]->name);
    }

    public function test_removed_schema_detected(): void
    {
        $source = new Catalog([schema('public'), schema('audit')]);
        $target = new Catalog([schema('public')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(0, $diff->addedSchemas);
        static::assertCount(1, $diff->removedSchemas);
        static::assertSame('audit', $diff->removedSchemas[0]->name);
    }

    public function test_removed_table_detected(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)]),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas);
        static::assertCount(1, $diff->modifiedSchemas[0]->removedTables);
        static::assertSame('users', $diff->modifiedSchemas[0]->removedTables[0]->name);
    }

    public function test_reverse_comparison_produces_inverse_diff(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)]),
                schema_table('orders', [schema_column_integer('id', false)]),
            ]),
        ]);

        $comparator = catalog_comparator();

        $upDiff = $comparator->compare($source, $target);
        $downDiff = $comparator->compare($target, $source);

        static::assertCount(1, $upDiff->modifiedSchemas[0]->addedTables);
        static::assertSame('orders', $upDiff->modifiedSchemas[0]->addedTables[0]->name);
        static::assertCount(0, $upDiff->modifiedSchemas[0]->removedTables);

        static::assertCount(0, $downDiff->modifiedSchemas[0]->addedTables);
        static::assertCount(1, $downDiff->modifiedSchemas[0]->removedTables);
        static::assertSame('orders', $downDiff->modifiedSchemas[0]->removedTables[0]->name);
    }

    public function test_sequence_added(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([
            schema('public', sequences: [
                schema_sequence('users_id_seq'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->addedSequences);
        static::assertSame('users_id_seq', $diff->modifiedSchemas[0]->addedSequences[0]->name);
    }

    public function test_sequence_cache_value_change_detected(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', cacheValue: 1),
            ]),
        ]);
        $target = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', cacheValue: 10),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedSequences);
    }

    public function test_sequence_column_type_change_detected(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', dataType: 'bigint'),
            ]),
        ]);
        $target = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', dataType: 'integer'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedSequences);
    }

    public function test_sequence_cycle_change_detected(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', cycle: false),
            ]),
        ]);
        $target = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', cycle: true),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedSequences);
    }

    public function test_sequence_max_value_change_detected(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1'),
            ]),
        ]);
        $target = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', maxValue: 9999),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedSequences);
    }

    public function test_sequence_min_value_change_detected(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', minValue: 1),
            ]),
        ]);
        $target = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', minValue: 0),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedSequences);
    }

    public function test_sequence_modification_detected(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('users_id_seq'),
            ]),
        ]);
        $target = new Catalog([
            schema('public', sequences: [
                schema_sequence('users_id_seq', incrementBy: 10),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas);
        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedSequences);
        static::assertSame('users_id_seq', $diff->modifiedSchemas[0]->modifiedSequences[0]->source->name);
    }

    public function test_sequence_no_change_when_identical(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', 'bigint', 1, 1, null, 1, false, 1, 'users', 'id'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_sequence_owned_by_column_change_detected(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', ownedByColumn: 'id'),
            ]),
        ]);
        $target = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', ownedByColumn: 'user_id'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedSequences);
    }

    public function test_sequence_owned_by_table_change_detected(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', ownedByTable: 'users'),
            ]),
        ]);
        $target = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', ownedByTable: 'accounts'),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedSequences);
    }

    public function test_sequence_removed(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('users_id_seq'),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->removedSequences);
    }

    public function test_sequence_start_value_change_detected(): void
    {
        $source = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', startValue: 1),
            ]),
        ]);
        $target = new Catalog([
            schema('public', sequences: [
                schema_sequence('s1', startValue: 100),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedSequences);
    }

    public function test_trigger_added(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)]),
            ]),
        ]);

        $trigger = new Trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');

        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$trigger]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedTriggers);
        static::assertSame('trg_audit', $tableDiff->addedTriggers[0]->name);
    }

    public function test_trigger_modified_events_change_treated_as_drop_and_add(): void
    {
        $sourceTrigger = new Trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');
        $targetTrigger = new Trigger(
            'trg_audit',
            'users',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT, TriggerEvent::UPDATE],
            'audit_fn',
        );

        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$sourceTrigger]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$targetTrigger]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedTriggers);
        static::assertCount(1, $tableDiff->removedTriggers);
    }

    public function test_trigger_modified_for_each_row_change_treated_as_drop_and_add(): void
    {
        $sourceTrigger = new Trigger(
            'trg_audit',
            'users',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT],
            'audit_fn',
            forEachRow: false,
        );
        $targetTrigger = new Trigger(
            'trg_audit',
            'users',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT],
            'audit_fn',
            forEachRow: true,
        );

        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$sourceTrigger]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$targetTrigger]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedTriggers);
        static::assertCount(1, $tableDiff->removedTriggers);
    }

    public function test_trigger_modified_function_name_change_treated_as_drop_and_add(): void
    {
        $sourceTrigger = new Trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');
        $targetTrigger = new Trigger(
            'trg_audit',
            'users',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT],
            'new_audit_fn',
        );

        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$sourceTrigger]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$targetTrigger]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedTriggers);
        static::assertCount(1, $tableDiff->removedTriggers);
    }

    public function test_trigger_modified_table_name_change_treated_as_drop_and_add(): void
    {
        $sourceTrigger = new Trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');
        $targetTrigger = new Trigger('trg_audit', 'accounts', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');

        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$sourceTrigger]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$targetTrigger]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedTriggers);
        static::assertCount(1, $tableDiff->removedTriggers);
    }

    public function test_trigger_modified_timing_change_treated_as_drop_and_add(): void
    {
        $sourceTrigger = new Trigger('trg_audit', 'users', TriggerTiming::BEFORE, [TriggerEvent::INSERT], 'audit_fn');
        $targetTrigger = new Trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');

        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$sourceTrigger]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$targetTrigger]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedTriggers);
        static::assertCount(1, $tableDiff->removedTriggers);
    }

    public function test_trigger_modified_when_condition_change_treated_as_drop_and_add(): void
    {
        $sourceTrigger = new Trigger(
            'trg_audit',
            'users',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT],
            'audit_fn',
            whenCondition: 'OLD.* IS DISTINCT FROM NEW.*',
        );
        $targetTrigger = new Trigger(
            'trg_audit',
            'users',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT],
            'audit_fn',
            whenCondition: 'NEW.active = true',
        );

        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$sourceTrigger]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$targetTrigger]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedTriggers);
        static::assertCount(1, $tableDiff->removedTriggers);
    }

    public function test_trigger_no_change_when_identical(): void
    {
        $trigger = new Trigger(
            'trg_audit',
            'users',
            TriggerTiming::AFTER,
            [TriggerEvent::INSERT],
            'audit_fn',
            true,
            'NEW.active = true',
        );

        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$trigger]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_trigger_removed(): void
    {
        $trigger = new Trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn');

        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)], triggers: [$trigger]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false)]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->removedTriggers);
        static::assertSame('trg_audit', $tableDiff->removedTriggers[0]->name);
    }

    public function test_unique_constraint_added(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false), schema_column_text('email')]),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    uniqueConstraints: [
                        schema_unique(['email']),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedUniqueConstraints);
    }

    public function test_unique_constraint_modified_nulls_not_distinct_change(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    uniqueConstraints: [
                        schema_unique(['email'], 'uq_email', false),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    uniqueConstraints: [
                        schema_unique(['email'], 'uq_email', true),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->addedUniqueConstraints);
        static::assertCount(1, $tableDiff->removedUniqueConstraints);
    }

    public function test_unique_constraint_removed(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    uniqueConstraints: [
                        schema_unique(['email']),
                    ],
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', tables: [
                schema_table('users', [schema_column_integer('id', false), schema_column_text('email')]),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        $tableDiff = $diff->modifiedSchemas[0]->modifiedTables[0];
        static::assertCount(1, $tableDiff->removedUniqueConstraints);
    }

    public function test_unique_constraint_without_name_uses_columns_for_matching(): void
    {
        $source = new Catalog([
            schema('public', tables: [
                schema_table(
                    'users',
                    [schema_column_integer('id', false), schema_column_text('email')],
                    uniqueConstraints: [
                        schema_unique(['email']),
                    ],
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_view_added(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([
            schema('public', views: [
                schema_view(
                    'active_users',
                    select(star())
                        ->from(table('users'))
                        ->where(eq(col('active'), literal(true)))
                        ->toSql(),
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->addedViews);
        static::assertSame('active_users', $diff->modifiedSchemas[0]->addedViews[0]->name);
    }

    public function test_view_is_updatable_change_detected(): void
    {
        $source = new Catalog([
            schema('public', views: [
                schema_view(
                    'active_users',
                    select(star())
                        ->from(table('users'))
                        ->where(eq(col('active'), literal(true)))
                        ->toSql(),
                    isUpdatable: false,
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', views: [
                schema_view(
                    'active_users',
                    select(star())
                        ->from(table('users'))
                        ->where(eq(col('active'), literal(true)))
                        ->toSql(),
                    isUpdatable: true,
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedViews);
    }

    public function test_view_modification_detected(): void
    {
        $source = new Catalog([
            schema('public', views: [
                schema_view(
                    'active_users',
                    select(star())
                        ->from(table('users'))
                        ->where(eq(col('active'), literal(true)))
                        ->toSql(),
                ),
            ]),
        ]);
        $target = new Catalog([
            schema('public', views: [
                schema_view(
                    'active_users',
                    select(star())
                        ->from(table('users'))
                        ->where(and_(eq(col('active'), literal(true)), eq(col('verified'), literal(true))))
                        ->toSql(),
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->modifiedViews);
    }

    public function test_view_no_change_when_identical(): void
    {
        $source = new Catalog([
            schema('public', views: [
                schema_view(
                    'active_users',
                    select(star())
                        ->from(table('users'))
                        ->where(eq(col('active'), literal(true)))
                        ->toSql(),
                    true,
                ),
            ]),
        ]);

        $diff = catalog_comparator()->compare($source, $source);

        static::assertTrue($diff->isEmpty());
    }

    public function test_view_removed(): void
    {
        $source = new Catalog([
            schema('public', views: [
                schema_view(
                    'active_users',
                    select(star())
                        ->from(table('users'))
                        ->where(eq(col('active'), literal(true)))
                        ->toSql(),
                ),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $diff = catalog_comparator()->compare($source, $target);

        static::assertCount(1, $diff->modifiedSchemas[0]->removedViews);
    }

    public function test_removed_view_drop_omits_if_exists_by_default(): void
    {
        $source = new Catalog([
            schema('public', views: [
                schema_view('active_users', select(literal(1))->toSql()),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $sqls = array_map(
            static fn(Sql $query): string => $query->toSql(),
            catalog_comparator()->compare($source, $target)->generate(),
        );

        static::assertContains('DROP VIEW active_users', $sqls);
        static::assertNotContains('DROP VIEW IF EXISTS active_users', $sqls);
    }

    public function test_removed_view_drop_gains_if_exists_when_flag_enabled(): void
    {
        $source = new Catalog([
            schema('public', views: [
                schema_view('active_users', select(literal(1))->toSql()),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $sqls = array_map(
            static fn(Sql $query): string => $query->toSql(),
            catalog_comparator(dropIfExists: true)->compare($source, $target)->generate(),
        );

        static::assertContains('DROP VIEW IF EXISTS active_users', $sqls);
        static::assertNotContains('DROP VIEW active_users', $sqls);
    }

    public function test_compare_override_forces_if_exists_on_default_off_comparator(): void
    {
        $source = new Catalog([
            schema('public', views: [
                schema_view('active_users', select(literal(1))->toSql()),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $sqls = array_map(
            static fn(Sql $query): string => $query->toSql(),
            catalog_comparator()->compare($source, $target, true)->generate(),
        );

        static::assertContains('DROP VIEW IF EXISTS active_users', $sqls);
        static::assertNotContains('DROP VIEW active_users', $sqls);
    }

    public function test_compare_override_suppresses_if_exists_on_default_on_comparator(): void
    {
        $source = new Catalog([
            schema('public', views: [
                schema_view('active_users', select(literal(1))->toSql()),
            ]),
        ]);
        $target = new Catalog([schema('public')]);

        $sqls = array_map(
            static fn(Sql $query): string => $query->toSql(),
            catalog_comparator(dropIfExists: true)->compare($source, $target, false)->generate(),
        );

        static::assertContains('DROP VIEW active_users', $sqls);
        static::assertNotContains('DROP VIEW IF EXISTS active_users', $sqls);
    }
}
