<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;

final class FilterTest extends FlowIntegrationTestCase
{
    public function test_multiple_filters(): void
    {
        static::assertSame(
            [
                ['id' => 4, 'name' => 'Smith', 'age' => 35],
            ],
            df()
                ->read(from_array([
                    ['id' => 1, 'name' => 'John', 'age' => 20],
                    ['id' => 2, 'name' => 'Jane', 'age' => 25],
                    ['id' => 3, 'name' => 'Doe', 'age' => 30],
                    ['id' => 4, 'name' => 'Smith', 'age' => 35],
                    ['id' => 5, 'name' => 'Brown', 'age' => 40],
                ]))
                ->filters([
                    ref('age')->greaterThanEqual(30),
                    ref('name')->startsWith('S'),
                ])
                ->fetch()
                ->toArray(),
        );
    }

    public function test_single_filter(): void
    {
        static::assertSame(
            [
                ['id' => 3, 'name' => 'Doe', 'age' => 30],
                ['id' => 4, 'name' => 'Smith', 'age' => 35],
                ['id' => 5, 'name' => 'Brown', 'age' => 40],
            ],
            df()
                ->read(from_array([
                    ['id' => 1, 'name' => 'John', 'age' => 20],
                    ['id' => 2, 'name' => 'Jane', 'age' => 25],
                    ['id' => 3, 'name' => 'Doe', 'age' => 30],
                    ['id' => 4, 'name' => 'Smith', 'age' => 35],
                    ['id' => 5, 'name' => 'Brown', 'age' => 40],
                ]))
                ->filter(ref('age')->greaterThanEqual(30))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_filter_over_a_file_source_guards_the_filter_above_it(): void
    {
        $file = $this->cacheDir->path() . '/guarded-filters.floe';

        df()
            ->read(from_array([['a' => 1, 'b' => 0], ['a' => 2, 'b' => 2]]))
            ->write(to_floe($file))
            ->run();

        static::assertSame(
            [['a' => 2, 'b' => 2]],
            df()
                ->read(from_floe($file))
                ->filter(ref('b')->notEquals(lit(0)))
                ->filter(ref('a')->divide(ref('b'))->greaterThan(lit(0)))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_an_int_column_equals_a_numeric_string_and_a_float(): void
    {
        static::assertSame(
            [1],
            df()
                ->read(from_array([['id' => 1], ['id' => 2]]))
                ->filter(ref('id')->equals(lit('1')))
                ->filter(ref('id')->equals(lit(1.0)))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_a_string_that_does_not_cast_to_an_int_column_throws(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage('value \'x\' cannot be cast to integer');

        df()
            ->read(from_array([['id' => 1]]))
            ->filter(ref('id')->equals(lit('x')))
            ->fetch();
    }
}
