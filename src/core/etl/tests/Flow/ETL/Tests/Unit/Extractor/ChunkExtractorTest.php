<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Config;
use Flow\ETL\Extractor\ChunkExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Tests\Double\AllRowTypesFakeExtractor;
use PHPUnit\Framework\TestCase;

final class ChunkExtractorTest extends TestCase
{
    public function test_chunk_extractor() : void
    {
        $extractor = new ChunkExtractor(new AllRowTypesFakeExtractor($batches = 5, $rowsNumber = 20), $chunkSize = 10);

        $this->assertCount(
            $rowsNumber / $chunkSize * $batches,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }
}
