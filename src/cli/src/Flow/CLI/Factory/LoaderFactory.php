<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\ETL\Adapter\Text\to_text;
use Flow\CLI\Options\FileFormat;
use Flow\ETL\Loader;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class LoaderFactory
{
    public function __construct(
        private Path $path,
        private FileFormat $format,
    ) {

    }

    public function get(InputInterface $input) : Loader
    {
        return match ($this->format) {
            FileFormat::CSV => (new CSVLoaderFactory($this->path))->get($input),
            FileFormat::JSON => (new JsonLoaderFactory($this->path))->get($input),
            FileFormat::XML => (new XMLLoaderFactory($this->path))->get($input),
            FileFormat::PARQUET => (new ParquetLoaderFactory($this->path))->get($input),
            FileFormat::TEXT => to_text($this->path),
        };
    }
}
