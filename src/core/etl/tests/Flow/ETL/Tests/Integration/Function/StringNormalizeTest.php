<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class StringNormalizeTest extends FlowTestCase
{
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
}
