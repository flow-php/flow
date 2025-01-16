<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class JsonEncodeTest extends FlowTestCase
{
    public function test_adding_json_as_object_from_string_entry() : void
    {
        (data_frame())
            ->read(
                from_array([['id' => 1]])
            )
            ->withEntry('json', lit(['id' => 1, 'name' => 'test']))
            ->withEntry('json', ref('json')->jsonEncode(\JSON_FORCE_OBJECT))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                [
                    'id' => 1,
                    'json' => ['id' => 1, 'name' => 'test'],
                ],
            ],
            $memory->dump()
        );
    }

    public function test_adding_json_from_string_entry() : void
    {
        (data_frame())
            ->read(
                from_array([['id' => 1]])
            )
            ->withEntry('json', lit([1, 2, 3]))
            ->withEntry('json', ref('json')->jsonEncode())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                [
                    'id' => 1,
                    'json' => [1, 2, 3],
                ],
            ],
            $memory->dump()
        );
    }
}
