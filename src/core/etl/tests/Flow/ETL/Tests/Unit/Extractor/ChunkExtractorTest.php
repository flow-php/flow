<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\ChunkExtractor;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowTestCase;

final class ChunkExtractorTest extends FlowTestCase
{
    public function test_chunk_extractor() : void
    {
        $extractor = new ChunkExtractor(new FakeExtractor($batches = 100), $chunkSize = 10);

        self::assertExtractedBatchesCount(
            $batches / $chunkSize,
            $extractor
        );
    }

    public function test_chunk_extractor_with_chunk_size_greater_than_() : void
    {
        $extractor = new ChunkExtractor(new FakeExtractor(total: 20), chunkSize: 25);

        self::assertExtractedBatchesCount(
            1,
            $extractor
        );
    }
}
