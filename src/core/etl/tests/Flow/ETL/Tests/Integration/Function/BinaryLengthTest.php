<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class BinaryLengthTest extends FlowTestCase
{
    public function test_binary_length() : void
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
                        ['text' => "\x00\x01\x02\xFF"],
                    ]
                )
            )
            ->withEntry('binary_length', ref('text')->binaryLength())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello', 'binary_length' => 5],
                ['text' => 'world🚀', 'binary_length' => 9],
                ['text' => 'café', 'binary_length' => 5],
                ['text' => 'नमस्ते', 'binary_length' => 18],
                ['text' => '', 'binary_length' => 0],
                ['text' => null, 'binary_length' => null],
                ['text' => 'a', 'binary_length' => 1],
                ['text' => str_repeat('x', 100), 'binary_length' => 100],
                ['text' => 'é', 'binary_length' => 2],
                ['text' => "\x00\x01\x02\xFF", 'binary_length' => 4],
            ],
            $memory->dump()
        );
    }

    public function test_binary_length_vs_character_length_comparison() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello'],
                        ['text' => 'café'],
                        ['text' => '🚀'],
                        ['text' => 'नमस्ते'],
                    ]
                )
            )
            ->withEntry('binary_length', ref('text')->binaryLength())
            ->withEntry('char_length', ref('text')->unicodeLength())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $result = $memory->dump();

        self::assertSame(5, $result[0]['binary_length']);
        self::assertSame(5, $result[0]['char_length']);

        self::assertSame(5, $result[1]['binary_length']);
        self::assertSame(4, $result[1]['char_length']);

        self::assertSame(4, $result[2]['binary_length']);
        self::assertSame(1, $result[2]['char_length']);

        self::assertSame(18, $result[3]['binary_length']);
        self::assertSame(3, $result[3]['char_length']);
    }

    public function test_binary_length_with_binary_data_analysis() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['data' => 'ASCII text', 'type' => 'text'],
                        ['data' => 'UTF-8: café', 'type' => 'utf8'],
                        ['data' => "\x89PNG\r\n\x1a\n", 'type' => 'binary'],
                        ['data' => "\xFF\xFE", 'type' => 'bom'],
                    ]
                )
            )
            ->withEntry('byte_size', ref('data')->binaryLength())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['data' => 'ASCII text', 'type' => 'text', 'byte_size' => 10],
                ['data' => 'UTF-8: café', 'type' => 'utf8', 'byte_size' => 12],
                ['data' => "\x89PNG\r\n\x1a\n", 'type' => 'binary', 'byte_size' => 8],
                ['data' => "\xFF\xFE", 'type' => 'bom', 'byte_size' => 2],
            ],
            $memory->dump()
        );
    }
}
