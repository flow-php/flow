<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\Exception\DataConversionException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter\{
    Int32DateConverter,
    Int32DateTimeConverter,
    Int64DateTimeConverter,
    Int96DateTimeConverter,
    JsonConverter,
    TimeConverter,
    UuidConverter};
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class DataConverter
{
    /**
     * @var array<string, null|Converter>
     */
    private array $cache;

    /**
     * @param array<Converter> $converters
     */
    public function __construct(private readonly array $converters, private readonly Options $options)
    {
        $this->cache = [];
    }

    public static function initialize(Options $options) : self
    {
        return new self(
            [
                new TimeConverter(),
                new Int32DateConverter(),
                new Int32DateTimeConverter(),
                new Int64DateTimeConverter(),
                new Int96DateTimeConverter(),
                new UuidConverter(),
                new JsonConverter(),
            ],
            $options
        );
    }

    public function fromParquetType(FlatColumn $column, mixed $data) : mixed
    {
        if ($data === null) {
            return null;
        }

        $flatPath = $column->flatPath();

        if (\array_key_exists($flatPath, $this->cache)) {
            if ($this->cache[$flatPath] === null) {
                return $data;
            }

            return $this->cache[$flatPath]->fromParquetType($data);
        }

        foreach ($this->converters as $converter) {
            if ($converter->isFor($column, $this->options)) {
                $this->cache[$flatPath] = $converter;

                try {
                    return $converter->fromParquetType($data);
                } catch (\Throwable $e) {
                    throw new DataConversionException(
                        "Failed to convert data from parquet type for column '{$flatPath}'. {$e->getMessage()}",
                        0,
                        $e
                    );
                }
            }
        }

        $this->cache[$flatPath] = null;

        return $data;
    }

    public function resolveConverter(FlatColumn $column) : ?Converter
    {
        $flatPath = $column->flatPath();

        if (\array_key_exists($flatPath, $this->cache)) {
            return $this->cache[$flatPath];
        }

        foreach ($this->converters as $converter) {
            if ($converter->isFor($column, $this->options)) {
                $this->cache[$flatPath] = $converter;

                return $converter;
            }
        }

        $this->cache[$flatPath] = null;

        return null;
    }

    public function toParquetType(FlatColumn $column, mixed $data) : mixed
    {
        if ($data === null) {
            return null;
        }

        $flatPath = $column->flatPath();

        if (\array_key_exists($flatPath, $this->cache)) {
            if ($this->cache[$flatPath] === null) {
                return $data;
            }

            return $this->cache[$flatPath]->toParquetType($data);
        }

        foreach ($this->converters as $converter) {
            if ($converter->isFor($column, $this->options)) {
                $this->cache[$flatPath] = $converter;

                return $converter->toParquetType($data);
            }
        }

        $this->cache[$flatPath] = null;

        return $data;
    }
}
