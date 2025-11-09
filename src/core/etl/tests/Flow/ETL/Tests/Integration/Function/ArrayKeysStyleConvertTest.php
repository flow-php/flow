<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{array_keys_style_convert, config, flow_context, from_array, ref, string_entry, to_memory};
use function Flow\ETL\DSL\{data_frame, row};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayKeysStyleConvertTest extends FlowTestCase
{
    public function test_array_keys_style_convert() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['camelCased' => 1, 'snake_cased' => 2, 'space word' => 3]],
                    ]
                )
            )
            ->withEntry('array', array_keys_style_convert(ref('array'), 'camel'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'array' => ['camelCased' => 1, 'snakeCased' => 2, 'spaceWord' => 3]],
            ],
            $memory->dump()
        );
    }

    public function test_array_keys_style_convert_in_strict_mode() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayKeysStyleConvert function requires non-null array');

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        array_keys_style_convert(ref('string'), 'camel')
            ->eval(
                row(string_entry('string', 'test')),
                $context
            );
    }
}
