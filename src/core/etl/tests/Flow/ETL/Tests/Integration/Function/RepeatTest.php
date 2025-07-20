<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class RepeatTest extends FlowTestCase
{
    public function test_repeat() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello', 'times' => 3],
                        ['text' => 'a', 'times' => 5],
                        ['text' => 'café', 'times' => 2],
                        ['text' => '🚀', 'times' => 4],
                        ['text' => '', 'times' => 3],
                        ['text' => 'test', 'times' => 0],
                        ['text' => 'hello', 'times' => -1],
                        ['text' => null, 'times' => 3],
                        ['text' => 'world', 'times' => 1],
                    ]
                )
            )
            ->withEntry('repeated', ref('text')->repeat(ref('times')))
            ->withEntry('repeated_fixed', ref('text')->repeat(2))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello', 'times' => 3, 'repeated' => 'hellohellohello', 'repeated_fixed' => 'hellohello'],
                ['text' => 'a', 'times' => 5, 'repeated' => 'aaaaa', 'repeated_fixed' => 'aa'],
                ['text' => 'café', 'times' => 2, 'repeated' => 'cafécafé', 'repeated_fixed' => 'cafécafé'],
                ['text' => '🚀', 'times' => 4, 'repeated' => '🚀🚀🚀🚀', 'repeated_fixed' => '🚀🚀'],
                ['text' => '', 'times' => 3, 'repeated' => '', 'repeated_fixed' => ''],
                ['text' => 'test', 'times' => 0, 'repeated' => '', 'repeated_fixed' => 'testtest'],
                ['text' => 'hello', 'times' => -1, 'repeated' => '', 'repeated_fixed' => 'hellohello'],
                ['text' => null, 'times' => 3, 'repeated' => null, 'repeated_fixed' => null],
                ['text' => 'world', 'times' => 1, 'repeated' => 'world', 'repeated_fixed' => 'worldworld'],
            ],
            $memory->dump()
        );
    }
}
