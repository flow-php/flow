<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class JsonDecodeTest extends FlowTestCase
{
    public function test_add_json_string_into_existing_reference() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]]],
                )
            )
            ->withEntry('json', lit('{"d": 4}'))
            ->withEntry('array', ref('array')->arrayMerge(ref('json')->jsonDecode()))
            ->drop('json')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4]],
            ],
            $memory->dump()
        );
    }
}
