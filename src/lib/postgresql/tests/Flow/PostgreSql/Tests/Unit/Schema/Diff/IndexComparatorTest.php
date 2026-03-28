<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use function Flow\PostgreSql\DSL\schema_index;

use Flow\PostgreSql\Schema\Diff\{GreedySimilarityRenameStrategy, IndexComparator, SimilarTextStrategy};
use Flow\PostgreSql\Schema\IndexMethod;
use PHPUnit\Framework\TestCase;

final class IndexComparatorTest extends TestCase
{
    public function test_added_index_detected() : void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare(
            [],
            [schema_index('idx_email', ['email'])],
        );

        self::assertCount(1, $result->added);
        self::assertSame('idx_email', $result->added[0]->name);
        self::assertSame([], $result->removed);
        self::assertSame([], $result->renamed);
    }

    public function test_ambiguous_rename_resolved_by_similarity() : void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare(
            [
                schema_index('idx_old_a', ['email']),
                schema_index('idx_old_b', ['email']),
            ],
            [
                schema_index('idx_new', ['email']),
            ],
        );

        self::assertCount(0, $result->added);
        self::assertCount(1, $result->removed);
        self::assertCount(1, $result->renamed);
    }

    public function test_index_with_same_name_different_columns_detected_as_change() : void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare(
            [schema_index('idx_search', ['email'])],
            [schema_index('idx_search', ['name'])],
        );

        self::assertCount(1, $result->added);
        self::assertSame(['name'], $result->added[0]->columns);
        self::assertCount(1, $result->removed);
        self::assertSame(['email'], $result->removed[0]->columns);
        self::assertSame([], $result->renamed);
    }

    public function test_index_with_same_name_different_method_detected_as_change() : void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare(
            [schema_index('idx_email', ['email'], method: IndexMethod::BTREE)],
            [schema_index('idx_email', ['email'], method: IndexMethod::HASH)],
        );

        self::assertCount(1, $result->added);
        self::assertSame(IndexMethod::HASH, $result->added[0]->method);
        self::assertCount(1, $result->removed);
        self::assertSame(IndexMethod::BTREE, $result->removed[0]->method);
        self::assertSame([], $result->renamed);
    }

    public function test_index_with_same_name_different_predicate_detected_as_change() : void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare(
            [schema_index('idx_active', ['active'], predicate: null)],
            [schema_index('idx_active', ['active'], predicate: 'active = true')],
        );

        self::assertCount(1, $result->added);
        self::assertSame('active = true', $result->added[0]->predicate);
        self::assertCount(1, $result->removed);
        self::assertNull($result->removed[0]->predicate);
        self::assertSame([], $result->renamed);
    }

    public function test_index_with_same_name_different_primary_detected_as_change() : void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare(
            [schema_index('idx_id', ['id'], primary: false)],
            [schema_index('idx_id', ['id'], primary: true)],
        );

        self::assertCount(1, $result->added);
        self::assertTrue($result->added[0]->primary);
        self::assertCount(1, $result->removed);
        self::assertFalse($result->removed[0]->primary);
        self::assertSame([], $result->renamed);
    }

    public function test_index_with_same_name_different_unique_detected_as_change() : void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare(
            [schema_index('idx_email', ['email'], unique: false)],
            [schema_index('idx_email', ['email'], unique: true)],
        );

        self::assertCount(1, $result->added);
        self::assertTrue($result->added[0]->unique);
        self::assertCount(1, $result->removed);
        self::assertFalse($result->removed[0]->unique);
        self::assertSame([], $result->renamed);
    }

    public function test_no_changes_for_identical_indexes() : void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare(
            [schema_index('idx_email', ['email'])],
            [schema_index('idx_email', ['email'])],
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
        self::assertSame([], $result->renamed);
    }

    public function test_removed_index_detected() : void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare(
            [schema_index('idx_email', ['email'])],
            [],
        );

        self::assertSame([], $result->added);
        self::assertCount(1, $result->removed);
        self::assertSame('idx_email', $result->removed[0]->name);
        self::assertSame([], $result->renamed);
    }

    public function test_rename_detected() : void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare(
            [schema_index('idx_old', ['email'])],
            [schema_index('idx_new', ['email'])],
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
        self::assertCount(1, $result->renamed);
        self::assertArrayHasKey('idx_old', $result->renamed);
        self::assertSame('idx_new', $result->renamed['idx_old']->name);
    }
}
