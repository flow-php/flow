<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, flow_context, int_entry, json_entry, ref};
use function Flow\ETL\DSL\row;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\{ArrayUnpack, ExecutionMode};
use Flow\ETL\Tests\FlowTestCase;

final class ArrayUnpackTest extends FlowTestCase
{
    public function test_array_unpack() : void
    {
        $row = row(int_entry('id', 1), json_entry('array_entry', [
            'status' => 'PENDING',
            'enabled' => true,
            'array' => ['foo' => 'bar'],
        ]));

        self::assertSame(
            [
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ],
            (new ArrayUnpack(ref('array_entry')))->eval($row, flow_context())
        );
    }

    public function test_array_unpack_with_null_in_strict_mode() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayUnpack requires non-null array and skipKeys');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        (new ArrayUnpack(ref('array_entry')))->eval(
            row(int_entry('id', 1), json_entry('array_entry', null)),
            $context
        );
    }
}
