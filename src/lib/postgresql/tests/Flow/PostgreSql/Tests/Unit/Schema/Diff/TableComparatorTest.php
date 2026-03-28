<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use function Flow\PostgreSql\DSL\{schema_column, schema_column_integer, schema_column_text, schema_primary_key, schema_table, schema_trigger};

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\Diff\{ConstraintComparator, GreedySimilarityRenameStrategy, IndexComparator, SimilarTextStrategy, TableComparator};
use Flow\PostgreSql\Schema\{IdentityGeneration, TriggerEvent, TriggerTiming};
use PHPUnit\Framework\TestCase;

final class TableComparatorTest extends TestCase
{
    public function test_ambiguous_column_rename_resolved_by_similarity() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [
            schema_column_integer('id', false),
            schema_column_text('col_a'),
            schema_column_text('col_b'),
        ]);
        $target = schema_table('users', [
            schema_column_integer('id', false),
            schema_column_text('col_c'),
        ]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(0, $diff->addedColumns);
        self::assertCount(1, $diff->removedColumns);
        self::assertCount(1, $diff->modifiedColumns);
        self::assertTrue($diff->modifiedColumns[0]->hasNameChanged());
    }

    public function test_column_added() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false), schema_column_text('email')]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->addedColumns);
        self::assertSame('email', $diff->addedColumns[0]->name);
    }

    public function test_column_default_changed() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column('status', ColumnType::text(), default: null)]);
        $target = schema_table('users', [schema_column('status', ColumnType::text(), default: "'active'")]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->modifiedColumns);
        self::assertTrue($diff->modifiedColumns[0]->hasDefaultChanged());
    }

    public function test_column_generation_changed() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column('full_name', ColumnType::text(), isGenerated: false)]);
        $target = schema_table('users', [schema_column('full_name', ColumnType::text(), isGenerated: true, generationExpression: "first_name || ' ' || last_name")]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->modifiedColumns);
        self::assertTrue($diff->modifiedColumns[0]->hasGenerationChanged());
    }

    public function test_column_identity_changed() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column('id', ColumnType::integer(), nullable: false, isIdentity: false)]);
        $target = schema_table('users', [schema_column('id', ColumnType::integer(), nullable: false, isIdentity: true, identityGeneration: IdentityGeneration::ALWAYS)]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->modifiedColumns);
        self::assertTrue($diff->modifiedColumns[0]->hasIdentityChanged());
    }

    public function test_column_nullable_changed() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column('email', ColumnType::text(), nullable: true)]);
        $target = schema_table('users', [schema_column('email', ColumnType::text(), nullable: false)]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->modifiedColumns);
        self::assertTrue($diff->modifiedColumns[0]->hasNullableChanged());
    }

    public function test_column_removed() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false), schema_column_text('email')]);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->removedColumns);
        self::assertSame('email', $diff->removedColumns[0]->name);
    }

    public function test_column_rename_detected() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false), schema_column_text('email')]);
        $target = schema_table('users', [schema_column_integer('id', false), schema_column_text('mail')]);

        $diff = $comparator->compare($source, $target);

        self::assertSame([], $diff->addedColumns);
        self::assertSame([], $diff->removedColumns);
        self::assertCount(1, $diff->modifiedColumns);
        self::assertTrue($diff->modifiedColumns[0]->hasNameChanged());
        self::assertSame('email', $diff->modifiedColumns[0]->source->name);
        self::assertSame('mail', $diff->modifiedColumns[0]->target->name);
    }

    public function test_column_type_changed() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column('email', ColumnType::text())]);
        $target = schema_table('users', [schema_column('email', ColumnType::varchar(255))]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->modifiedColumns);
        self::assertTrue($diff->modifiedColumns[0]->hasTypeChanged());
    }

    public function test_identical_tables_produce_empty_diff() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $table = schema_table('users', [schema_column_integer('id', false), schema_column_text('email')]);

        $diff = $comparator->compare($table, $table);

        self::assertTrue($diff->isEmpty());
    }

    public function test_primary_key_added() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)], primaryKey: schema_primary_key(['id']));

        $diff = $comparator->compare($source, $target);

        self::assertNotNull($diff->addedPrimaryKey);
        self::assertSame(['id'], $diff->addedPrimaryKey->columns);
        self::assertNull($diff->removedPrimaryKey);
    }

    public function test_primary_key_changed() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false), schema_column_text('email')], primaryKey: schema_primary_key(['id']));
        $target = schema_table('users', [schema_column_integer('id', false), schema_column_text('email')], primaryKey: schema_primary_key(['id', 'email']));

        $diff = $comparator->compare($source, $target);

        self::assertNotNull($diff->addedPrimaryKey);
        self::assertNotNull($diff->removedPrimaryKey);
    }

    public function test_primary_key_removed() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)], primaryKey: schema_primary_key(['id']));
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = $comparator->compare($source, $target);

        self::assertNull($diff->addedPrimaryKey);
        self::assertNotNull($diff->removedPrimaryKey);
        self::assertSame(['id'], $diff->removedPrimaryKey->columns);
    }

    public function test_trigger_added() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)]);
        $target = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn'),
        ]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->addedTriggers);
        self::assertSame('trg_audit', $diff->addedTriggers[0]->name);
    }

    public function test_trigger_changed_events() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn'),
        ]);
        $target = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT, TriggerEvent::UPDATE], 'audit_fn'),
        ]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->addedTriggers);
        self::assertCount(1, $diff->removedTriggers);
    }

    public function test_trigger_changed_for_each_row() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', forEachRow: false),
        ]);
        $target = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', forEachRow: true),
        ]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->addedTriggers);
        self::assertCount(1, $diff->removedTriggers);
    }

    public function test_trigger_changed_function_name() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn'),
        ]);
        $target = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'log_fn'),
        ]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->addedTriggers);
        self::assertCount(1, $diff->removedTriggers);
    }

    public function test_trigger_changed_table_name() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn'),
        ]);
        $target = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'accounts', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn'),
        ]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->addedTriggers);
        self::assertCount(1, $diff->removedTriggers);
    }

    public function test_trigger_changed_timing() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn'),
        ]);
        $target = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::BEFORE, [TriggerEvent::INSERT], 'audit_fn'),
        ]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->addedTriggers);
        self::assertCount(1, $diff->removedTriggers);
    }

    public function test_trigger_changed_when_condition() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', whenCondition: null),
        ]);
        $target = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn', whenCondition: 'NEW.active = true'),
        ]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->addedTriggers);
        self::assertCount(1, $diff->removedTriggers);
    }

    public function test_trigger_removed() : void
    {
        $comparator = new TableComparator(new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())), new ConstraintComparator(), new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));
        $source = schema_table('users', [schema_column_integer('id', false)], triggers: [
            schema_trigger('trg_audit', 'users', TriggerTiming::AFTER, [TriggerEvent::INSERT], 'audit_fn'),
        ]);
        $target = schema_table('users', [schema_column_integer('id', false)]);

        $diff = $comparator->compare($source, $target);

        self::assertCount(1, $diff->removedTriggers);
        self::assertSame('trg_audit', $diff->removedTriggers[0]->name);
    }
}
