<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\ChunkExtractor;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\{Config, FlowContext};
use PHPUnit\Framework\TestCase;

final class ChunkExtractorTest extends TestCase
{
    public function test_chunk_extractor() : void
    {
        $extractor = new ChunkExtractor(new FakeExtractor($batches = 100), $chunkSize = 10);

        self::assertCount(
            $batches / $chunkSize,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_chunk_extractor_with_chunk_size_greater_than_() : void
    {
        $extractor = new ChunkExtractor(new FakeExtractor(total: 20), chunkSize: 25);

        self::assertCount(
            1,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }
}
