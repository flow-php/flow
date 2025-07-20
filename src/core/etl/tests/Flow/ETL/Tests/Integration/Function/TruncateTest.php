<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class TruncateTest extends FlowTestCase
{
    public function test_truncate() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world', 'length' => 5],
                        ['text' => 'short', 'length' => 10],
                        ['text' => 'café au lait', 'length' => 4],
                        ['text' => 'hello🚀world', 'length' => 6],
                        ['text' => '', 'length' => 5],
                        ['text' => null, 'length' => 5],
                    ]
                )
            )
            ->withEntry('truncated', ref('text')->truncate(ref('length')))
            ->withEntry('truncated_custom', ref('text')->truncate(ref('length'), '>>'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'length' => 5, 'truncated' => 'he...', 'truncated_custom' => 'hel>>'],
                ['text' => 'short', 'length' => 10, 'truncated' => 'short', 'truncated_custom' => 'short'],
                ['text' => 'café au lait', 'length' => 4, 'truncated' => 'c...', 'truncated_custom' => 'ca>>'],
                ['text' => 'hello🚀world', 'length' => 6, 'truncated' => 'hel...', 'truncated_custom' => 'hell>>'],
                ['text' => '', 'length' => 5, 'truncated' => '', 'truncated_custom' => ''],
                ['text' => null, 'length' => 5, 'truncated' => null, 'truncated_custom' => null],
            ],
            $memory->dump()
        );
    }
}
