<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class UnicodeLengthTest extends FlowTestCase
{
    public function test_unicode_length() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello'],
                        ['text' => 'world🚀'],
                        ['text' => 'café'],
                        ['text' => 'नमस्ते'],
                        ['text' => ''],
                        ['text' => null],
                        ['text' => 'a'],
                        ['text' => str_repeat('x', 100)],
                        ['text' => 'é'],
                        ['text' => "e\u{0301}"],
                        ['text' => '👋🏻'],
                        ['text' => '👨‍👩‍👧‍👦'],
                    ]
                )
            )
            ->withEntry('unicode_length', ref('text')->unicodeLength())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello', 'unicode_length' => 5],
                ['text' => 'world🚀', 'unicode_length' => 6],
                ['text' => 'café', 'unicode_length' => 4],
                ['text' => 'नमस्ते', 'unicode_length' => 3],
                ['text' => '', 'unicode_length' => 0],
                ['text' => null, 'unicode_length' => null],
                ['text' => 'a', 'unicode_length' => 1],
                ['text' => str_repeat('x', 100), 'unicode_length' => 100],
                ['text' => 'é', 'unicode_length' => 1],
                ['text' => "e\u{0301}", 'unicode_length' => 1],
                ['text' => '👋🏻', 'unicode_length' => 1],
                ['text' => '👨‍👩‍👧‍👦', 'unicode_length' => 1],
            ],
            $memory->dump()
        );
    }
}
