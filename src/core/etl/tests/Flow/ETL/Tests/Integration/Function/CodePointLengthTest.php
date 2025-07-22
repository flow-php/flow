<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class CodePointLengthTest extends FlowTestCase
{
    public function test_code_point_length() : void
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
                        ['text' => '𝐇'],
                    ]
                )
            )
            ->withEntry('code_point_length', ref('text')->codePointLength())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello', 'code_point_length' => 5],
                ['text' => 'world🚀', 'code_point_length' => 6],
                ['text' => 'café', 'code_point_length' => 4],
                ['text' => 'नमस्ते', 'code_point_length' => 6],
                ['text' => '', 'code_point_length' => 0],
                ['text' => null, 'code_point_length' => null],
                ['text' => 'a', 'code_point_length' => 1],
                ['text' => str_repeat('x', 100), 'code_point_length' => 100],
                ['text' => 'é', 'code_point_length' => 1],
                ['text' => "e\u{0301}", 'code_point_length' => 1],
                ['text' => '👋🏻', 'code_point_length' => 2],
                ['text' => '👨‍👩‍👧‍👦', 'code_point_length' => 7],
                ['text' => '𝐇', 'code_point_length' => 1],
            ],
            $memory->dump()
        );
    }
}
