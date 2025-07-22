<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class StringWidthTest extends FlowTestCase
{
    public function test_width() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello'],
                        ['text' => '中文'],
                        ['text' => 'ひらがな'],
                        ['text' => '한글'],
                        ['text' => '🚀'],
                        ['text' => ''],
                        ['text' => null],
                        ['text' => 'a'],
                        ['text' => 'hello中文'],
                        ['text' => '！'],
                    ]
                )
            )
            ->withEntry('width', ref('text')->stringWidth())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello', 'width' => 5],
                ['text' => '中文', 'width' => 4],
                ['text' => 'ひらがな', 'width' => 8],
                ['text' => '한글', 'width' => 4],
                ['text' => '🚀', 'width' => 2],
                ['text' => '', 'width' => 0],
                ['text' => null, 'width' => null],
                ['text' => 'a', 'width' => 1],
                ['text' => 'hello中文', 'width' => 9],
                ['text' => '！', 'width' => 2],
            ],
            $memory->dump()
        );
    }
}
