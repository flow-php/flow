<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ChunkTest extends FlowTestCase
{
    public function test_chunk() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world', 'size' => 3],
                        ['text' => 'test', 'size' => 2],
                        ['text' => 'café', 'size' => 2],
                        ['text' => 'hello🚀world', 'size' => 4],
                        ['text' => '', 'size' => 3],
                        ['text' => 'a', 'size' => 1],
                        ['text' => 'longtext', 'size' => 10],
                        ['text' => 'chunk', 'size' => 0],
                        ['text' => null, 'size' => 3],
                    ]
                )
            )
            ->withEntry('chunked', ref('text')->chunk(ref('size')))
            ->withEntry('chunked_fixed', ref('text')->chunk(2))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'size' => 3, 'chunked' => ['hel', 'lo ', 'wor', 'ld'], 'chunked_fixed' => ['he', 'll', 'o ', 'wo', 'rl', 'd']],
                ['text' => 'test', 'size' => 2, 'chunked' => ['te', 'st'], 'chunked_fixed' => ['te', 'st']],
                ['text' => 'café', 'size' => 2, 'chunked' => ['ca', 'fé'], 'chunked_fixed' => ['ca', 'fé']],
                ['text' => 'hello🚀world', 'size' => 4, 'chunked' => ['hell', 'o🚀wo', 'rld'], 'chunked_fixed' => ['he', 'll', 'o🚀', 'wo', 'rl', 'd']],
                ['text' => '', 'size' => 3, 'chunked' => [], 'chunked_fixed' => []],
                ['text' => 'a', 'size' => 1, 'chunked' => ['a'], 'chunked_fixed' => ['a']],
                ['text' => 'longtext', 'size' => 10, 'chunked' => ['longtext'], 'chunked_fixed' => ['lo', 'ng', 'te', 'xt']],
                ['text' => 'chunk', 'size' => 0, 'chunked' => [], 'chunked_fixed' => ['ch', 'un', 'k']],
                ['text' => null, 'size' => 3, 'chunked' => null, 'chunked_fixed' => null],
            ],
            $memory->dump()
        );
    }
}
