<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\Exception\DataConversionException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter\DecimalConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int32DateConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int64DateTimeConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int96DateTimeConverter;
use Flow\Parquet\ParquetFile\Data\Converter\JsonConverter;
use Flow\Parquet\ParquetFile\Data\Converter\TimeConverter;
use Flow\Parquet\ParquetFile\Data\Converter\UuidConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Throwable;

use function array_key_exists;

final class DataConverter
{
    /**
     * @var array<string, null|Converter>
     */
    private array $cache;

    /**
     * @param list<class-string<Converter>> $converters
     */
    public function __construct(
        private readonly array $converters,
        private readonly Options $options,
    ) {
        $this->cache = [];
    }

    public static function initialize(Options $options): self
    {
        return new self([
            TimeConverter::class,
            Int32DateConverter::class,
            Int64DateTimeConverter::class,
            Int96DateTimeConverter::class,
            DecimalConverter::class,
            UuidConverter::class,
            JsonConverter::class,
        ], $options);
    }

    public function fromParquetType(FlatColumn $column, mixed $data): mixed
    {
        if ($data === null) {
            return null;
        }

        $converter = $this->resolveConverter($column);

        if ($converter === null) {
            return $data;
        }

        try {
            return $converter->fromParquetType($data);
        } catch (Throwable $e) {
            throw new DataConversionException(
                "Failed to convert data from parquet type for column '{$column->flatPath()}'. {$e->getMessage()}",
                0,
                $e,
            );
        }
    }

    public function resolveConverter(FlatColumn $column): ?Converter
    {
        $flatPath = $column->flatPath();

        if (array_key_exists($flatPath, $this->cache)) {
            return $this->cache[$flatPath];
        }

        foreach ($this->converters as $class) {
            // @mago-ignore analysis:possibly-static-access-on-interface
            $converter = $class::forColumn($column, $this->options);

            if ($converter !== null) {
                return $this->cache[$flatPath] = $converter;
            }
        }

        return $this->cache[$flatPath] = null;
    }

    public function toParquetType(FlatColumn $column, mixed $data): mixed
    {
        if ($data === null) {
            return null;
        }

        $converter = $this->resolveConverter($column);

        return $converter === null ? $data : $converter->toParquetType($data);
    }
}
