<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\GreedySimilarityRenameStrategy;
use Flow\PostgreSql\Schema\Diff\IndexComparator;
use Flow\PostgreSql\Schema\Diff\SimilarTextStrategy;
use Flow\PostgreSql\Schema\IndexMethod;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_index;

final class IndexComparatorTest extends TestCase
{
    public function test_added_index_detected(): void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare([], [schema_index('idx_email', ['email'])]);

        static::assertCount(1, $result->added);
        static::assertSame('idx_email', $result->added[0]->name);
        static::assertSame([], $result->removed);
        static::assertSame([], $result->renamed);
    }

    public function test_ambiguous_rename_resolved_by_similarity(): void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare([
            schema_index('idx_old_a', ['email']),
            schema_index('idx_old_b', ['email']),
        ], [
            schema_index('idx_new', ['email']),
        ]);

        static::assertCount(0, $result->added);
        static::assertCount(1, $result->removed);
        static::assertCount(1, $result->renamed);
    }

    public function test_index_with_same_name_different_columns_detected_as_change(): void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare([schema_index('idx_search', ['email'])], [schema_index('idx_search', ['name'])]);

        static::assertCount(1, $result->added);
        static::assertSame(['name'], $result->added[0]->columns);
        static::assertCount(1, $result->removed);
        static::assertSame(['email'], $result->removed[0]->columns);
        static::assertSame([], $result->renamed);
    }

    public function test_index_with_same_name_different_method_detected_as_change(): void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare([schema_index(
            'idx_email',
            ['email'],
            method: IndexMethod::BTREE,
        )], [schema_index('idx_email', ['email'], method: IndexMethod::HASH)]);

        static::assertCount(1, $result->added);
        static::assertSame(IndexMethod::HASH, $result->added[0]->method);
        static::assertCount(1, $result->removed);
        static::assertSame(IndexMethod::BTREE, $result->removed[0]->method);
        static::assertSame([], $result->renamed);
    }

    public function test_index_with_same_name_different_predicate_detected_as_change(): void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare([schema_index('idx_active', ['active'], predicate: null)], [schema_index(
            'idx_active',
            ['active'],
            predicate: 'active = true',
        )]);

        static::assertCount(1, $result->added);
        static::assertSame('active = true', $result->added[0]->predicate);
        static::assertCount(1, $result->removed);
        static::assertNull($result->removed[0]->predicate);
        static::assertSame([], $result->renamed);
    }

    public function test_index_with_same_name_different_primary_detected_as_change(): void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare([schema_index('idx_id', ['id'], primary: false)], [schema_index(
            'idx_id',
            ['id'],
            primary: true,
        )]);

        static::assertCount(1, $result->added);
        static::assertTrue($result->added[0]->primary);
        static::assertCount(1, $result->removed);
        static::assertFalse($result->removed[0]->primary);
        static::assertSame([], $result->renamed);
    }

    public function test_index_with_same_name_different_unique_detected_as_change(): void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare([schema_index('idx_email', ['email'], unique: false)], [schema_index(
            'idx_email',
            ['email'],
            unique: true,
        )]);

        static::assertCount(1, $result->added);
        static::assertTrue($result->added[0]->unique);
        static::assertCount(1, $result->removed);
        static::assertFalse($result->removed[0]->unique);
        static::assertSame([], $result->renamed);
    }

    public function test_no_changes_for_identical_indexes(): void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare([schema_index('idx_email', ['email'])], [schema_index('idx_email', ['email'])]);

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
        static::assertSame([], $result->renamed);
    }

    public function test_removed_index_detected(): void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare([schema_index('idx_email', ['email'])], []);

        static::assertSame([], $result->added);
        static::assertCount(1, $result->removed);
        static::assertSame('idx_email', $result->removed[0]->name);
        static::assertSame([], $result->renamed);
    }

    public function test_rename_detected(): void
    {
        $comparator = new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy()));

        $result = $comparator->compare([schema_index('idx_old', ['email'])], [schema_index('idx_new', ['email'])]);

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
        static::assertCount(1, $result->renamed);
        static::assertArrayHasKey('idx_old', $result->renamed);
        static::assertSame('idx_new', $result->renamed['idx_old']->name);
    }
}
