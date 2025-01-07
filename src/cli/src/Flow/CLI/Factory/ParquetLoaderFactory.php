<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\ETL\Adapter\Parquet\to_parquet;
use Flow\ETL\Adapter\Parquet\ParquetLoader;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class ParquetLoaderFactory
{
    public function __construct(
        private Path $path,
    ) {
    }

    public function get(InputInterface $input) : ParquetLoader
    {
        return to_parquet($this->path);
    }
}
