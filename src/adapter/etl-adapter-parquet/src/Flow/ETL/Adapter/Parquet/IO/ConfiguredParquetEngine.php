<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetEngine;
use ReflectionClass;
use SensitiveParameter;

use function array_filter;
use function array_values;
use function extension_loaded;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function is_a;

/**
 * A parquet engine as its class plus the configuration it normalizes to.
 */
final readonly class ConfiguredParquetEngine implements Describable
{
    /**
     * This package's and flow-php/parquet's composer.json.
     */
    public const array EXTENSIONS = ['bcmath', 'json', 'zlib'];

    /**
     * The codec extensions a file may need: a worker loads the ones the parent reads with.
     */
    public const array CODECS = ['brotli', 'lz4', 'snappy', 'zstd'];

    public function __construct(
        private ParquetEngine $engine,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $class = type_string()->assert($data['engine'] ?? null);

        if (!is_a($class, ParquetEngine::class, true) || (new ReflectionClass($class))->isAbstract()) {
            throw InvalidArgumentException::because('%s is not a concrete ParquetEngine', $class);
        }

        // @mago-ignore analysis:possibly-static-access-on-interface
        return new self($class::fromArray(type_array()->assert($data['configuration'] ?? null)));
    }

    public function describe(): ?Description
    {
        $configuration = $this->engine->normalize();

        return (
            $configuration === null
                ? null
                : new Description(
                    self::class,
                    ['engine' => $this->engine::class, 'configuration' => $configuration],
                    extensions: [
                        ...self::EXTENSIONS,
                        ...array_values(array_filter(self::CODECS, extension_loaded(...))),
                    ],
                )
        );
    }

    public function engine(): ParquetEngine
    {
        return $this->engine;
    }
}
