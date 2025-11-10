<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, int_entry, json_entry, ref};
use function Flow\ETL\DSL\row;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayReverseTest extends FlowTestCase
{
    public function test_array_reverse_array_entry() : void
    {
        self::assertSame(
            [5, 3, 10, 4],
            ref('a')->arrayReverse()
                ->eval(
                    row(json_entry('a', [4, 10, 3, 5])),
                    flow_context()
                )
        );
    }

    public function test_array_reverse_in_strict_mode() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayReverse function requires non-null array');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        ref('a')->arrayReverse()
            ->eval(
                row(int_entry('a', 123)),
                $context
            );
    }

    public function test_array_reverse_non_array_entry() : void
    {
        self::assertNull(
            ref('a')->arrayReverse()
                ->eval(
                    row(int_entry('a', 123)),
                    flow_context()
                )
        );
    }
}
