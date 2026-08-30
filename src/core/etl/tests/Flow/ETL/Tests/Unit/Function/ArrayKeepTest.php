<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ArrayKeepTest extends FlowTestCase
{
    public function test_array_keep(): void
    {
        static::assertSame([1 => 2], ref('list')->arrayKeep(lit(2))->eval(row(['list' => [1, 2]]), flow_context()));
    }

    public function test_array_keep_by_entry_reference(): void
    {
        static::assertSame(
            [1 => 2],
            ref('list')->arrayKeep(ref('int'))->eval(row(['list' => [1, 2], 'int' => 2]), flow_context()),
        );
    }

    public function test_array_keep_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        $context = flow_context(config());
        ref('map')->arrayKeep(lit(1))->eval(row(['map' => 'test']), $context);
    }

    public function test_array_keep_not_existing_value(): void
    {
        static::assertSame([], ref('list')->arrayKeep(lit(5))->eval(row(['list' => [1, 2]]), flow_context()));
    }

    public function test_array_keep_on_non_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        ref('map')->arrayKeep(lit(1))->eval(row(['map' => 'test']), flow_context());
    }
}
