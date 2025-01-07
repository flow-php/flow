<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\ETL\Adapter\Text\from_text;
use Flow\CLI\Options\FileFormat;
use Flow\ETL\Extractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class ExtractorFactory
{
    public function __construct(
        private Path $path,
        private FileFormat $format,
    ) {
    }

    public function get(InputInterface $input) : Extractor
    {
        return match ($this->format) {
            FileFormat::CSV => (new CSVExtractorFactory($this->path))->get($input),
            FileFormat::JSON => (new JsonExtractorFactory($this->path))->get($input),
            FileFormat::XML => (new XMLExtractorFactory($this->path))->get($input),
            FileFormat::PARQUET => (new ParquetExtractorFactory($this->path))->get($input),
            FileFormat::TEXT => from_text($this->path),
        };
    }
}
