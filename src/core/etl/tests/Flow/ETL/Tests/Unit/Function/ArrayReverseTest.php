<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ArrayReverseTest extends FlowTestCase
{
    public function test_array_reverse_array_entry(): void
    {
        static::assertSame(
            [5, 3, 10, 4],
            ref('a')->arrayReverse()->eval(row(json_entry('a', [4, 10, 3, 5])), flow_context()),
        );
    }

    public function test_array_reverse_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $context = flow_context(config());
        ref('a')->arrayReverse()->eval(row(int_entry('a', 123)), $context);
    }

    public function test_array_reverse_non_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        ref('a')->arrayReverse()->eval(row(int_entry('a', 123)), flow_context());
    }
}
