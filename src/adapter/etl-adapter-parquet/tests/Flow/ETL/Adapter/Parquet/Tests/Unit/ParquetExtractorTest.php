<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\Path;
use Flow\Parquet\Options;
use PHPUnit\Framework\TestCase;

final class ParquetExtractorTest extends TestCase
{
    public function test_using_offset_with_pattern_path() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset can be used only with single file path, not with pattern');

        new ParquetExtractor(
            new Path('/tmp/*.parquet'),
            Options::default(),
            offset: 100
        );
    }
}
