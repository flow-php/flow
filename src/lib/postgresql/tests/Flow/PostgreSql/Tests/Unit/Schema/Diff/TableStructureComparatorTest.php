<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use function Flow\PostgreSql\DSL\{schema_column_integer, schema_column_text, schema_column_varchar, schema_index, schema_primary_key, schema_table, schema_trigger, schema_unique};

use Flow\PostgreSql\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\Schema\Diff\{GreedySimilarityRenameStrategy, SimilarTextStrategy, TableStructureComparator};
use Flow\PostgreSql\Schema\{PartitionStrategy, TriggerEvent, TriggerTiming};
use PHPUnit\Framework\TestCase;

final class TableStructureComparatorTest extends TestCase
{
    public function test_detect_rename_ambiguous_resolved_by_similarity() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->detectTableRenames(
            [schema_table('table_c', [schema_column_integer('id', false)])],
            [schema_table('table_a', [schema_column_integer('id', false)]), schema_table('table_b', [schema_column_integer('id', false)])],
        );

        self::assertCount(0, $result->added);
        self::assertCount(1, $result->removed);
        self::assertCount(1, $result->renamed);
    }

    public function test_detect_rename_not_triggered_when_columns_differ() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->detectTableRenames(
            [schema_table('new_users', [schema_column_integer('id', false), schema_column_text('name')])],
            [schema_table('old_users', [schema_column_integer('id', false)])],
        );

        self::assertCount(1, $result->added);
        self::assertCount(1, $result->removed);
        self::assertNull($result->renamed);
    }

    public function test_detect_rename_when_structure_identical() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->detectTableRenames(
            [schema_table('new_users', [schema_column_integer('id', false), schema_column_text('name')], schema_primary_key(['id']))],
            [schema_table('old_users', [schema_column_integer('id', false), schema_column_text('name')], schema_primary_key(['id']))],
        );

        self::assertCount(0, $result->added);
        self::assertCount(0, $result->removed);
        self::assertCount(1, $result->renamed);
        self::assertArrayHasKey('public.old_users', $result->renamed);
        self::assertSame('new_users', $result->renamed['public.old_users']->name);
    }

    public function test_identical_tables_are_structurally_equal() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')], schema_primary_key(['id']));
        $b = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')], schema_primary_key(['id']));

        self::assertTrue($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_differ_by_column_count() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('t', [schema_column_integer('id', false)]);
        $b = schema_table('t', [schema_column_integer('id', false), schema_column_text('name')]);

        self::assertFalse($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_differ_by_column_nullable() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('t', [schema_column_integer('id', false)]);
        $b = schema_table('t', [schema_column_integer('id', true)]);

        self::assertFalse($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_differ_by_column_type() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('t', [schema_column_integer('id', false), schema_column_text('name')]);
        $b = schema_table('t', [schema_column_integer('id', false), schema_column_varchar('name', 255)]);

        self::assertFalse($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_differ_by_inherits() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('t', [schema_column_integer('id', false)]);
        $b = schema_table('t', [schema_column_integer('id', false)], inherits: ['parent']);

        self::assertFalse($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_differ_by_partition_strategy() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('t', [schema_column_integer('id', false)]);
        $b = schema_table('t', [schema_column_integer('id', false)], partitionStrategy: PartitionStrategy::RANGE, partitionColumns: ['id']);

        self::assertFalse($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_differ_by_primary_key() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('t', [schema_column_integer('id', false)], schema_primary_key(['id']));
        $b = schema_table('t', [schema_column_integer('id', false)]);

        self::assertFalse($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_differ_by_tablespace() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('t', [schema_column_integer('id', false)]);
        $b = schema_table('t', [schema_column_integer('id', false)], tablespace: 'fast_storage');

        self::assertFalse($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_differ_by_unlogged() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('t', [schema_column_integer('id', false)]);
        $b = schema_table('t', [schema_column_integer('id', false)], unlogged: true);

        self::assertFalse($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_not_equal_across_different_schemas() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('t', [schema_column_integer('id', false)], schema: 'public');
        $b = schema_table('t', [schema_column_integer('id', false)], schema: 'audit');

        self::assertFalse($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_with_different_constraint_names_are_structurally_equal() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table(
            'old',
            [schema_column_integer('id', false), schema_column_text('email')],
            uniqueConstraints: [schema_unique(['email'], 'uq_old_email')],
            checkConstraints: [new CheckConstraint("email <> ''", 'chk_old')],
        );
        $b = schema_table(
            'new',
            [schema_column_integer('id', false), schema_column_text('email')],
            uniqueConstraints: [schema_unique(['email'], 'uq_new_email')],
            checkConstraints: [new CheckConstraint("email <> ''", 'chk_new')],
        );

        self::assertTrue($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_with_different_index_names_are_structurally_equal() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('old', [schema_column_integer('id', false), schema_column_text('email')], indexes: [
            schema_index('idx_old_email', ['email']),
        ]);
        $b = schema_table('new', [schema_column_integer('id', false), schema_column_text('email')], indexes: [
            schema_index('idx_new_email', ['email']),
        ]);

        self::assertTrue($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_with_different_index_structure_are_not_equal() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('old', [schema_column_integer('id', false), schema_column_text('email')], indexes: [
            schema_index('idx_old', ['email']),
        ]);
        $b = schema_table('new', [schema_column_integer('id', false), schema_column_text('email')], indexes: [
            schema_index('idx_new', ['email'], unique: true),
        ]);

        self::assertFalse($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_with_different_names_are_structurally_equal() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table('old_users', [schema_column_integer('id', false), schema_column_text('name')]);
        $b = schema_table('new_users', [schema_column_integer('id', false), schema_column_text('name')]);

        self::assertTrue($comparator->haveEqualStructure($a, $b));
    }

    public function test_tables_with_different_trigger_names_are_structurally_equal() : void
    {
        $comparator = new TableStructureComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $a = schema_table(
            'old',
            [schema_column_integer('id', false)],
            triggers: [schema_trigger('trg_old', 'old', TriggerTiming::BEFORE, [TriggerEvent::INSERT], 'my_func')],
        );
        $b = schema_table(
            'new',
            [schema_column_integer('id', false)],
            triggers: [schema_trigger('trg_new', 'new', TriggerTiming::BEFORE, [TriggerEvent::INSERT], 'my_func')],
        );

        self::assertTrue($comparator->haveEqualStructure($a, $b));
    }
}
