<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Output;

use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\ETL\Loader;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use RuntimeException;

use function Flow\ETL\Adapter\Parquet\to_parquet;

if (!function_exists('Flow\ETL\Adapter\Parquet\to_parquet')) {
    throw new RuntimeException(
        'Flow\ETL\Adapter\Parquet\to_parquet function is not available. Make sure that composer require flow-php/etl-adapter-parquet dependency is present in your composer.json.',
    );
}

final readonly class ParquetOutput implements Output
{
    public function __construct(
        private ?Options $options = null,
        private Compressions $compressions = Compressions::SNAPPY,
        private ?Schema $schema = null,
    ) {}

    public function loader(Path $path, Filesystem $filesystem): Loader
    {
        $loader = to_parquet($path, filesystem: $filesystem)->withCompressions($this->compressions);

        if ($this->options !== null) {
            $loader->withOptions($this->options);
        }

        if ($this->schema !== null) {
            $loader->withSchema($this->schema);
        }

        return $loader;
    }

    public function type(): Type
    {
        return Type::PARQUET;
    }
}
