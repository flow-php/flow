<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PartitionExtractor};
use Flow\ETL\Filesystem\Path;
use Flow\ETL\{Exception\RuntimeException, Extractor, FlowContext};

final class AvroExtractor implements Extractor, FileExtractor, LimitableExtractor, PartitionExtractor
{
    use Limitable;
    use PathFiltering;

    public function __construct(private readonly Path $path)
    {
        throw new RuntimeException('Avro integration was abandoned due to lack of availability of good Avro libraries.');
    }

    public function extract(FlowContext $context) : \Generator
    {
        yield;
    }

    public function source() : Path
    {
        return $this->path;
    }
}
