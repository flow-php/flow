<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\concat_ws;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class ConcatWithSeparatorTest extends FlowTestCase
{
    public function test_concat_on_list_of_strings_and_null(): void
    {
        data_frame()
            ->read(from_array([
                ['tags' => ['a', 'b', 'c', 'd']],
            ]))
            ->withEntry('tags', ref('tags')->concatWithSeparator(lit(', '), lit(null)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['tags' => 'a, b, c, d'],
            ],
            $memory->dump(),
        );
    }

    public function test_concat_on_non_string_value(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
            ]))
            ->withEntry('concat', concat_ws(lit(','), ref('id'), lit(null)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'concat' => '1'],
                ['id' => 2, 'concat' => '2'],
            ],
            $memory->dump(),
        );
    }

    public function test_concat_on_nulls(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1, 'array' => ['field' => 'value']],
                ['id' => 2],
            ]))
            ->withEntry('concat', optional(concat_ws(lit(null), lit(null))))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'concat' => null],
                ['id' => 2, 'concat' => null],
            ],
            $memory->dump(),
        );
    }

    public function test_concat_with_separator(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
            ]))
            ->withEntry('concat', concat_ws(lit('->'), lit('id'), ref('id')))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'concat' => 'id->1'],
                ['id' => 2, 'concat' => 'id->2'],
            ],
            $memory->dump(),
        );
    }
}
