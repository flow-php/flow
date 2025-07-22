<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref};
use Flow\ETL\Tests\FlowTestCase;

final class EnsureStartTest extends FlowTestCase
{
    public function test_ensure_start_in_dataframe_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['url' => 'example.com', 'prefix' => 'https://'],
                ['url' => 'https://github.com', 'prefix' => 'https://'],
                ['url' => 'ftp://files.example.com', 'prefix' => 'https://'],
            ]))
            ->withEntry('normalized_url', ref('url')->ensureStart(ref('prefix')));

        self::assertEquals(
            [
                ['url' => 'example.com', 'prefix' => 'https://', 'normalized_url' => 'https://example.com'],
                ['url' => 'https://github.com', 'prefix' => 'https://', 'normalized_url' => 'https://github.com'],
                ['url' => 'ftp://files.example.com', 'prefix' => 'https://', 'normalized_url' => 'https://ftp://files.example.com'],
            ],
            $df->fetch()->toArray()
        );
    }
}
