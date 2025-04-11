<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Output;

use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\Filesystem\DSL\{path_memory, path_stdout};
use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\ETL\{Loader, Schema};
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;

if (!function_exists('Flow\ETL\Adapter\Parquet\to_parquet')) {
    throw new \RuntimeException('Flow\ETL\Adapter\Parquet\to_parquet function is not available. Make sure that composer require flow-php/etl-adapter-json dependency is present in your composer.json.');
}

final readonly class ParquetOutput implements Output
{
    public function __construct(
        private ?Options $options = null,
        private Compressions $compressions = Compressions::SNAPPY,
        private ?Schema $schema = null,
    ) {
    }

    public function memoryLoader(string $id) : Loader
    {
        $loader = to_parquet(path_memory($id, ['stream' => 'temp']))
            ->withCompressions($this->compressions);

        if ($this->options !== null) {
            $loader->withOptions($this->options);
        }

        if ($this->schema !== null) {
            $loader->withSchema($this->schema);
        }

        return $loader;
    }

    public function stdoutLoader() : Loader
    {
        $loader = to_parquet(path_stdout(['stream' => 'output']))
            ->withCompressions($this->compressions);

        if ($this->options !== null) {
            $loader->withOptions($this->options);
        }

        if ($this->schema !== null) {
            $loader->withSchema($this->schema);
        }

        return $loader;
    }

    public function type() : Type
    {
        return Type::PARQUET;
    }
}
