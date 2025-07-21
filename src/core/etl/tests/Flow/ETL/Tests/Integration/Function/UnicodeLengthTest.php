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

    public function test_unicode_length_internationalization_scenarios() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'Hello', 'lang' => 'en'],
                        ['text' => 'Bonjour', 'lang' => 'fr'],
                        ['text' => 'Hola', 'lang' => 'es'],
                        ['text' => 'Привет', 'lang' => 'ru'],
                        ['text' => 'こんにちは', 'lang' => 'ja'],
                        ['text' => '你好', 'lang' => 'zh'],
                        ['text' => '안녕하세요', 'lang' => 'ko'],
                        ['text' => 'مرحبا', 'lang' => 'ar'],
                        ['text' => 'नमस्ते', 'lang' => 'hi'],
                        ['text' => 'สวัสดี', 'lang' => 'th'],
                    ]
                )
            )
            ->withEntry('grapheme_count', ref('text')->unicodeLength())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'Hello', 'lang' => 'en', 'grapheme_count' => 5],
                ['text' => 'Bonjour', 'lang' => 'fr', 'grapheme_count' => 7],
                ['text' => 'Hola', 'lang' => 'es', 'grapheme_count' => 4],
                ['text' => 'Привет', 'lang' => 'ru', 'grapheme_count' => 6],
                ['text' => 'こんにちは', 'lang' => 'ja', 'grapheme_count' => 5],
                ['text' => '你好', 'lang' => 'zh', 'grapheme_count' => 2],
                ['text' => '안녕하세요', 'lang' => 'ko', 'grapheme_count' => 5],
                ['text' => 'مرحبا', 'lang' => 'ar', 'grapheme_count' => 5],
                ['text' => 'नमस्ते', 'lang' => 'hi', 'grapheme_count' => 3],
                ['text' => 'สวัสดี', 'lang' => 'th', 'grapheme_count' => 4],
            ],
            $memory->dump()
        );
    }

    public function test_unicode_length_vs_other_length_functions_comparison() : void
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
                    ]
                )
            )
            ->withEntry('unicode_length', ref('text')->unicodeLength())
            ->withEntry('char_length', ref('text')->length())
            ->withEntry('binary_length', ref('text')->binaryLength())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $result = $memory->dump();

        self::assertSame(5, $result[0]['unicode_length']);
        self::assertSame(5, $result[0]['char_length']);
        self::assertSame(5, $result[0]['binary_length']);

        self::assertSame(4, $result[1]['unicode_length']);
        self::assertSame(4, $result[1]['char_length']);
        self::assertSame(5, $result[1]['binary_length']);

        self::assertSame(1, $result[2]['unicode_length']);
        self::assertSame(1, $result[2]['char_length']);
        self::assertSame(4, $result[2]['binary_length']);

        self::assertSame(1, $result[3]['unicode_length']);
        self::assertSame(1, $result[3]['char_length']);
        self::assertSame(3, $result[3]['binary_length']);

        self::assertSame(1, $result[4]['unicode_length']);
        self::assertSame(1, $result[4]['char_length']);
        self::assertSame(25, $result[4]['binary_length']);
    }
}
