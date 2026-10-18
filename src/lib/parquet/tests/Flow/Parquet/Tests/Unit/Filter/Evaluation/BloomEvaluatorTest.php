<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Evaluation;

use Flow\Parquet\Filter\Truth;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\BloomTruth;
use Flow\Parquet\Tests\Double\InMemoryIndexes;
use Flow\Parquet\Tests\Mother\SbbfMother;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_any;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_not;

final class BloomEvaluatorTest extends TestCase
{
    public function test_an_equality_whose_literal_is_absent_is_never(): void
    {
        static::assertSame(Truth::Never, BloomTruth::of(
            parquet_column('i')->equals(4),
            new InMemoryIndexes(bloomFilters: ['i' => [SbbfMother::with(FlatColumn::int64('i'), 1, 2, 3)]]),
        ));
        static::assertSame(Truth::Never, BloomTruth::of(
            parquet_column('s')->equals('d'),
            new InMemoryIndexes(bloomFilters: ['s' => [SbbfMother::with(FlatColumn::string('s'), 'a', 'b')]]),
        ));
    }

    public function test_an_equality_whose_literal_is_present_is_maybe(): void
    {
        static::assertSame(Truth::Maybe, BloomTruth::of(
            parquet_column('i')->equals(2),
            new InMemoryIndexes(bloomFilters: ['i' => [SbbfMother::with(FlatColumn::int64('i'), 1, 2, 3)]]),
        ));
    }

    public function test_in_is_never_only_when_every_literal_is_absent(): void
    {
        $indexes = new InMemoryIndexes(bloomFilters: ['i' => [SbbfMother::with(FlatColumn::int64('i'), 1, 2, 3)]]);

        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_column('i')->in([5, 3, 9]), $indexes));
        static::assertSame(Truth::Never, BloomTruth::of(parquet_column('i')->in([5, 9]), $indexes));
    }

    public function test_a_negated_equality_is_maybe_and_reads_no_filter(): void
    {
        $indexes = new InMemoryIndexes(bloomFilters: ['i' => [SbbfMother::with(FlatColumn::int64('i'), 1)]]);

        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_not(parquet_column('i')->equals(4)), $indexes));
        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_not(parquet_column('i')->in([4, 5])), $indexes));
        static::assertSame([], $indexes->reads);
    }

    public function test_a_doubly_negated_equality_is_positive_again(): void
    {
        static::assertSame(Truth::Never, BloomTruth::of(
            parquet_not(parquet_not(parquet_column('i')->equals(4))),
            new InMemoryIndexes(bloomFilters: ['i' => [SbbfMother::with(FlatColumn::int64('i'), 1)]]),
        ));
    }

    public function test_ranges_null_checks_and_prefixes_are_maybe_and_read_no_filter(): void
    {
        $indexes = new InMemoryIndexes(bloomFilters: [
            'i' => [SbbfMother::with(FlatColumn::int64('i'), 1)],
            's' => [SbbfMother::with(FlatColumn::string('s'), 'a')],
        ]);

        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_column('i')->greaterThan(4), $indexes));
        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_column('i')->isNull(), $indexes));
        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_column('s')->startsWith('b'), $indexes));
        static::assertSame([], $indexes->reads);
    }

    public function test_an_equality_no_physical_value_satisfies_is_never_without_reading_a_filter(): void
    {
        $indexes = new InMemoryIndexes();

        static::assertSame(Truth::Never, BloomTruth::of(parquet_column('i')->equals(1.5), $indexes));
        static::assertSame([], $indexes->reads);
    }

    public function test_a_chunk_without_a_filter_or_a_column_the_file_lacks_is_maybe(): void
    {
        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_column('i')->equals(4), new InMemoryIndexes()));
        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_column('missing')->equals(4), new InMemoryIndexes()));
    }

    public function test_a_zero_is_looked_up_with_both_signs(): void
    {
        static::assertSame(Truth::Maybe, BloomTruth::of(
            parquet_column('f')->equals(0.0),
            new InMemoryIndexes(bloomFilters: ['f' => [SbbfMother::with(FlatColumn::double('f'), -0.0)]]),
        ));
        static::assertSame(Truth::Maybe, BloomTruth::of(
            parquet_column('f')->equals(-0.0),
            new InMemoryIndexes(bloomFilters: ['f' => [SbbfMother::with(FlatColumn::double('f'), 0.0)]]),
        ));
        static::assertSame(Truth::Never, BloomTruth::of(
            parquet_column('f')->equals(0.0),
            new InMemoryIndexes(bloomFilters: ['f' => [SbbfMother::with(FlatColumn::double('f'), 1.5)]]),
        ));
    }

    public function test_all_is_never_when_one_child_is_never_and_any_only_when_every_child_is(): void
    {
        $indexes = new InMemoryIndexes(bloomFilters: ['i' => [SbbfMother::with(FlatColumn::int64('i'), 1)]]);
        $absent = parquet_column('i')->equals(4);
        $present = parquet_column('i')->equals(1);

        static::assertSame(Truth::Never, BloomTruth::of(parquet_all($present, $absent), $indexes));
        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_any($present, $absent), $indexes));
        static::assertSame(Truth::Never, BloomTruth::of(
            parquet_any($absent, parquet_column('i')->equals(5)),
            $indexes,
        ));
        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_not(parquet_all($present, $absent)), $indexes));
        static::assertSame(Truth::Maybe, BloomTruth::of(parquet_not(parquet_any($present, $absent)), $indexes));
    }
}
