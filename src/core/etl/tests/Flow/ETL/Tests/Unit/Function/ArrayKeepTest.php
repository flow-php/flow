<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, int_entry, list_entry, lit, ref, string_entry};
use function Flow\ETL\DSL\row;
use function Flow\Types\DSL\{type_integer, type_list};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayKeepTest extends FlowTestCase
{
    public function test_array_keep() : void
    {
        self::assertSame(
            [1 => 2],
            ref('list')->arrayKeep(lit(2))
                ->eval(
                    row(list_entry('list', [1, 2], type_list(type_integer()))),
                    flow_context()
                )
        );
    }

    public function test_array_keep_by_entry_reference() : void
    {
        self::assertSame(
            [1 => 2],
            ref('list')->arrayKeep(ref('int'))
                ->eval(
                    row(
                        list_entry('list', [1, 2], type_list(type_integer())),
                        int_entry('int', 2)
                    ),
                    flow_context()
                )
        );
    }

    public function test_array_keep_in_strict_mode() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayKeep function requires non-null array');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        ref('map')->arrayKeep(lit(1))
            ->eval(
                row(string_entry('map', 'test')),
                $context
            );
    }

    public function test_array_keep_not_existing_value() : void
    {
        self::assertSame(
            [],
            ref('list')->arrayKeep(lit(5))
                ->eval(
                    row(list_entry('list', [1, 2], type_list(type_integer()))),
                    flow_context()
                )
        );
    }

    public function test_array_keep_on_non_array() : void
    {
        self::assertNull(
            ref('map')->arrayKeep(lit(1))
                ->eval(
                    row(string_entry('map', 'test')),
                    flow_context()
                )
        );
    }
}
