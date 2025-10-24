<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\batches;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowTestCase;

final class BatchExtractorTest extends FlowTestCase
{
    public function test_chunk_extractor() : void
    {
        $extractor = batches(new FakeExtractor($batches = 100), $chunkSize = 10);

        self::assertExtractedBatchesCount($batches / $chunkSize, $extractor);
    }

    public function test_chunk_extractor_with_chunk_size_greater_than_() : void
    {
        $extractor = batches(new FakeExtractor(total: 20), size: 25);

        self::assertExtractedBatchesCount(1, $extractor);
    }
}
