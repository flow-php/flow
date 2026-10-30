<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

use const JSON_FORCE_OBJECT;

final class JsonEncodeTest extends FlowTestCase
{
    public function test_adding_json_as_object_from_string_entry(): void
    {
        data_frame()
            ->read(from_array([['id' => 1]]))
            ->withColumn('json', lit(['id' => 1, 'name' => 'test']))
            ->withColumn('json', ref('json')->jsonEncode(JSON_FORCE_OBJECT))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                [
                    'id' => 1,
                    'json' => ['id' => 1, 'name' => 'test'],
                ],
            ],
            $memory->dump(),
        );
    }

    public function test_adding_json_from_string_entry(): void
    {
        data_frame()
            ->read(from_array([['id' => 1]]))
            ->withColumn('json', lit([1, 2, 3]))
            ->withColumn('json', ref('json')->jsonEncode())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                [
                    'id' => 1,
                    'json' => [1, 2, 3],
                ],
            ],
            $memory->dump(),
        );
    }
}
