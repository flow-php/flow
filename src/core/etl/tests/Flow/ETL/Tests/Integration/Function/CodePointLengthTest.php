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

    public function test_code_point_length_unicode_analysis_scenarios() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'ASCII', 'type' => 'basic'],
                        ['text' => 'café', 'type' => 'accented'],
                        ['text' => "e\u{0301}", 'type' => 'decomposed'],
                        ['text' => '🚀', 'type' => 'emoji'],
                        ['text' => '👋🏻', 'type' => 'emoji_modifier'],
                        ['text' => '👨‍👩‍👧‍👦', 'type' => 'emoji_sequence'],
                        ['text' => '𝐇', 'type' => 'surrogate_pair'],
                        ['text' => 'नमस्ते', 'type' => 'complex_script'],
                    ]
                )
            )
            ->withEntry('code_points', ref('text')->codePointLength())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'ASCII', 'type' => 'basic', 'code_points' => 5],
                ['text' => 'café', 'type' => 'accented', 'code_points' => 4],
                ['text' => "e\u{0301}", 'type' => 'decomposed', 'code_points' => 1],
                ['text' => '🚀', 'type' => 'emoji', 'code_points' => 1],
                ['text' => '👋🏻', 'type' => 'emoji_modifier', 'code_points' => 2],
                ['text' => '👨‍👩‍👧‍👦', 'type' => 'emoji_sequence', 'code_points' => 7],
                ['text' => '𝐇', 'type' => 'surrogate_pair', 'code_points' => 1],
                ['text' => 'नमस्ते', 'type' => 'complex_script', 'code_points' => 6],
            ],
            $memory->dump()
        );
    }

    public function test_code_point_length_vs_other_length_functions_comparison() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello'],
                        ['text' => 'café'],
                        ['text' => '🚀'],
                        ['text' => "e\u{0301}"],
                        ['text' => '👨‍👩‍👧‍👦'],
                        ['text' => '𝐇'],
                    ]
                )
            )
            ->withEntry('code_point_length', ref('text')->codePointLength())
            ->withEntry('unicode_length', ref('text')->unicodeLength())
            ->withEntry('binary_length', ref('text')->binaryLength())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $result = $memory->dump();

        self::assertSame(5, $result[0]['code_point_length']);
        self::assertSame(5, $result[0]['unicode_length']);
        self::assertSame(5, $result[0]['binary_length']);

        self::assertSame(4, $result[1]['code_point_length']);
        self::assertSame(4, $result[1]['unicode_length']);
        self::assertSame(5, $result[1]['binary_length']);

        self::assertSame(1, $result[2]['code_point_length']);
        self::assertSame(1, $result[2]['unicode_length']);
        self::assertSame(4, $result[2]['binary_length']);

        self::assertSame(1, $result[3]['code_point_length']);
        self::assertSame(1, $result[3]['unicode_length']);
        self::assertSame(3, $result[3]['binary_length']);

        self::assertSame(7, $result[4]['code_point_length']);
        self::assertSame(1, $result[4]['unicode_length']);
        self::assertSame(25, $result[4]['binary_length']);

        self::assertSame(1, $result[5]['code_point_length']);
        self::assertSame(1, $result[5]['unicode_length']);
        self::assertSame(4, $result[5]['binary_length']);
    }
}
