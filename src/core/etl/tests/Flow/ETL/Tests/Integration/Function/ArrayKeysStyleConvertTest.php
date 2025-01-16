<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{array_keys_style_convert, from_array, ref, to_memory};
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
}
