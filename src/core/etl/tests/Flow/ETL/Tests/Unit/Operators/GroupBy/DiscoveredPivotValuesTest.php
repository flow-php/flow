<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\GroupBy;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\GroupBy\DiscoveredPivotValues;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidTypeException;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final class DiscoveredPivotValuesTest extends FlowTestCase
{
    public function test_a_max_values_below_one_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('discover_pivot_values() must allow at least one value, given: 0');

        new DiscoveredPivotValues(0);
    }

    public function test_it_reads_the_frame_so_an_earlier_filter_narrows_the_values(): void
    {
        static::assertSame(
            ['a'],
            (new DiscoveredPivotValues())->resolve(
                df()
                    ->read(from_array([['k' => 'a'], ['k' => 'b']]))
                    ->filter(ref('k')->equals(lit('a')))
                    ->get(),
                ref('k'),
            )->all(),
        );
    }

    public function test_it_reads_the_frame_so_a_derived_column_is_visible(): void
    {
        static::assertSame(
            ['a!', 'b!'],
            (new DiscoveredPivotValues())->resolve(
                df()
                    ->read(from_array([['k' => 'a'], ['k' => 'b']]))
                    ->withColumn('k2', concat(ref('k'), lit('!')))
                    ->get(),
                ref('k2'),
            )->all(),
        );
    }

    public function test_it_refuses_a_source_with_no_values_to_declare(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('found no values in column "country"');

        (new DiscoveredPivotValues())->resolve(df()->read(from_array([['country' => null]]))->get(), ref('country'));
    }

    public function test_it_refuses_more_distinct_values_than_the_bound_allows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('found more than 1 distinct values in column "country"');

        (new DiscoveredPivotValues(1))->resolve(
            df()->read(from_array([['country' => 'PL'], ['country' => 'US']]))->get(),
            ref('country'),
        );
    }

    public function test_numeric_looking_strings_are_ordered_by_bytes_as_sort_by_does(): void
    {
        static::assertSame(
            ['10', '100', '9', 'B'],
            (new DiscoveredPivotValues())->resolve(
                df()->read(from_array([['code' => '9'], ['code' => '10'], ['code' => 'B'], ['code' => '100']]))->get(),
                ref('code'),
            )->all(),
        );
    }

    public function test_only_string_and_integer_values_can_become_pivot_columns(): void
    {
        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage('Expected type "integer|string", got "float".');

        (new DiscoveredPivotValues())->resolve(df()->read(from_array([['code' => 1.5]]))->get(), ref('code'));
    }

    public function test_integers_are_ordered_numerically(): void
    {
        static::assertSame(
            [9, 10, 100],
            (new DiscoveredPivotValues())->resolve(
                df()->read(from_array([['code' => 100], ['code' => 9], ['code' => 10]]))->get(),
                ref('code'),
            )->all(),
        );
    }

    public function test_the_discovered_values_are_distinct_null_free_and_sorted(): void
    {
        static::assertSame(
            ['China', 'PL', 'US'],
            (new DiscoveredPivotValues())->resolve(
                df()->read(from_array([
                    ['country' => 'US'],
                    ['country' => 'PL'],
                    ['country' => 'US'],
                    ['country' => null],
                    ['country' => 'China'],
                ]))->get(),
                ref('country'),
            )->all(),
        );
    }
}
