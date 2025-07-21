<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class StringNormalizeTest extends FlowTestCase
{
    public function test_normalize_internationalization_data_cleanup() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['name' => 'José', 'country' => 'ES'],
                        ['name' => "Jose\u{0301}", 'country' => 'ES'],
                        ['name' => 'François', 'country' => 'FR'],
                        ['name' => 'Björn', 'country' => 'SE'],
                        ['name' => 'Müller', 'country' => 'DE'],
                    ]
                )
            )
            ->withEntry('normalized_name', ref('name')->stringNormalize())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $result = $memory->dump();

        self::assertSame('José', $result[0]['normalized_name']);
        self::assertSame('José', $result[1]['normalized_name']);
        self::assertSame('François', $result[2]['normalized_name']);
        self::assertSame('Björn', $result[3]['normalized_name']);
        self::assertSame('Müller', $result[4]['normalized_name']);

        self::assertSame($result[0]['normalized_name'], $result[1]['normalized_name']);
    }

    public function test_normalize_nfc() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello'],
                        ['text' => 'café'],
                        ['text' => "e\u{0301}"],
                        ['text' => 'Việt Nam'],
                        ['text' => ''],
                        ['text' => null],
                        ['text' => 'مرحبا'],
                        ['text' => 'καλημέρα'],
                        ['text' => '👋🏻'],
                    ]
                )
            )
            ->withEntry('normalized', ref('text')->stringNormalize())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello', 'normalized' => 'hello'],
                ['text' => 'café', 'normalized' => 'café'],
                ['text' => "e\u{0301}", 'normalized' => 'é'],
                ['text' => 'Việt Nam', 'normalized' => 'Việt Nam'],
                ['text' => '', 'normalized' => ''],
                ['text' => null, 'normalized' => null],
                ['text' => 'مرحبا', 'normalized' => 'مرحبا'],
                ['text' => 'καλημέρα', 'normalized' => 'καλημέρα'],
                ['text' => '👋🏻', 'normalized' => '👋🏻'],
            ],
            $memory->dump()
        );
    }

    public function test_normalize_nfd() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'café'],
                        ['text' => 'é'],
                        ['text' => 'Müller'],
                    ]
                )
            )
            ->withEntry('nfc', ref('text')->stringNormalize())
            ->withEntry('nfd', ref('text')->stringNormalize(\Normalizer::NFD))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $result = $memory->dump();

        self::assertNotSame($result[0]['nfc'], $result[0]['nfd']);
        self::assertNotSame($result[1]['nfc'], $result[1]['nfd']);
        self::assertNotSame($result[2]['nfc'], $result[2]['nfd']);

        self::assertNotEmpty($result[0]['nfc']);
        self::assertNotEmpty($result[0]['nfd']);
    }

    public function test_normalize_text_comparison_scenario() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['original' => 'café', 'type' => 'composed'],
                        ['original' => "cafe\u{0301}", 'type' => 'decomposed'],
                        ['original' => 'résumé', 'type' => 'mixed'],
                        ['original' => "resume\u{0301}\u{0301}", 'type' => 'double_decomposed'],
                    ]
                )
            )
            ->withEntry('normalized', ref('original')->stringNormalize(\Normalizer::NFC))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $result = $memory->dump();

        self::assertSame('café', $result[0]['normalized']);
        self::assertSame('café', $result[1]['normalized']);
        self::assertSame('résumé', $result[2]['normalized']);

        self::assertSame($result[0]['normalized'], $result[1]['normalized']);
    }

    public function test_normalize_with_dynamic_form() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => "e\u{0301}", 'form' => \Normalizer::NFC],
                        ['text' => "e\u{0301}", 'form' => \Normalizer::NFD],
                        ['text' => 'ﬃ', 'form' => \Normalizer::NFKC],
                        ['text' => 'ﬃ', 'form' => \Normalizer::NFKD],
                    ]
                )
            )
            ->withEntry('normalized', ref('text')->stringNormalize(ref('form')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $result = $memory->dump();

        self::assertSame('é', $result[0]['normalized']);
        self::assertSame("e\u{0301}", $result[1]['normalized']);
        self::assertSame('ffi', $result[2]['normalized']);
        self::assertSame('ffi', $result[3]['normalized']);
    }
}
