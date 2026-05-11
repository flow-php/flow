<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;

final class EnsureEndTest extends FlowTestCase
{
    public function test_ensure_end_in_dataframe_operations(): void
    {
        $df = df()
            ->from(from_array([
                ['filename' => 'document', 'extension' => '.txt'],
                ['filename' => 'image.png', 'extension' => '.txt'],
                ['filename' => 'config.yml', 'extension' => '.txt'],
            ]))
            ->withEntry('normalized_filename', ref('filename')->ensureEnd(ref('extension')));

        static::assertEquals(
            [
                ['filename' => 'document', 'extension' => '.txt', 'normalized_filename' => 'document.txt'],
                ['filename' => 'image.png', 'extension' => '.txt', 'normalized_filename' => 'image.png.txt'],
                ['filename' => 'config.yml', 'extension' => '.txt', 'normalized_filename' => 'config.yml.txt'],
            ],
            $df->fetch()->toArray(),
        );
    }
}
