<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline\Optimizer\Doubles;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\PartitionFiltering;
use Flow\ETL\Extractor\PartitionsExtractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

final class FilePartitionExtractorSpy implements Extractor, FileExtractor, PartitionsExtractor
{
    use PartitionFiltering;

    public function __construct(
        private readonly Path $path
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        yield new Rows();
    }

    public function source() : Path
    {
        return $this->path;
    }
}
