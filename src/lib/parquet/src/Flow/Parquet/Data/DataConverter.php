<?php declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\Data\Converter\BytesStringConverter;
use Flow\Parquet\Data\Converter\Int32DateConverter;
use Flow\Parquet\Data\Converter\Int32DateTimeConverter;
use Flow\Parquet\Data\Converter\Int64DateTimeConverter;
use Flow\Parquet\Data\Converter\Int96DateTimeConverter;
use Flow\Parquet\Data\Converter\TimeConverter;
use Flow\Parquet\Exception\DataConversionException;
use Flow\Parquet\Options;
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
                new BytesStringConverter(),
            ],
            $options
        );
    }

    public function fromParquetType(FlatColumn $column, mixed $data) : mixed
    {
        if ($data === null) {
            return null;
        }

        if (\array_key_exists($column->flatPath(), $this->cache)) {
            if ($this->cache[$column->flatPath()] === null) {
                return $data;
            }

            /** @psalm-suppress PossiblyNullReference */
            return $this->cache[$column->flatPath()]->fromParquetType($data);
        }

        foreach ($this->converters as $converter) {
            if ($converter->isFor($column, $this->options)) {
                $this->cache[$column->flatPath()] = $converter;

                try {
                    return $converter->fromParquetType($data);
                } catch (\Throwable $e) {
                    throw new DataConversionException(
                        "Failed to convert data from parquet type for column '{$column->flatPath()}'. {$e->getMessage()}",
                        0,
                        $e
                    );
                }
            }
        }

        $this->cache[$column->flatPath()] = null;

        return $data;
    }

    public function toParquetType(FlatColumn $column, mixed $data) : mixed
    {
        if (\array_key_exists($column->flatPath(), $this->cache)) {
            if ($this->cache[$column->flatPath()] === null) {
                return $data;
            }

            /** @psalm-suppress PossiblyNullReference */
            return $this->cache[$column->flatPath()]->toParquetType($data);
        }

        foreach ($this->converters as $converter) {
            if ($converter->isFor($column, $this->options)) {
                $this->cache[$column->flatPath()] = $converter;

                return $converter->toParquetType($data);
            }
        }

        $this->cache[$column->flatPath()] = null;

        return $data;
    }
}
