<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class StrReplaceTest extends FlowTestCase
{
    public function test_str_replace(): void
    {
        data_frame()
            ->read(from_array([
                ['key' => 'value'],
            ]))
            ->withColumn('str_replace', ref('key')->strReplace('e', 'es'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['key' => 'value', 'str_replace' => 'values'],
            ],
            $memory->dump(),
        );
    }
}
