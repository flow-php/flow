<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class StrPadTest extends FlowTestCase
{
    public function test_strpad(): void
    {
        data_frame()
            ->read(from_array([
                ['key' => 'value'],
            ]))
            ->withColumn('strpad', ref('key')->strPad(10, '*'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['key' => 'value', 'strpad' => 'value*****'],
            ],
            $memory->dump(),
        );
    }
}
