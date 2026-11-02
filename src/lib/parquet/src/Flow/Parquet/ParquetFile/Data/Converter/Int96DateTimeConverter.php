<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use DateTimeImmutable;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function Flow\Parquet\floor_div;
use function sprintf;

final readonly class Int96DateTimeConverter implements Converter
{
    private StoredTemporalConverter $stored;

    public function __construct()
    {
        $this->stored = StoredTemporalConverter::int96();
    }

    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        return $column->type() === PhysicalType::INT96 && $options->get(Option::INT_96_AS_DATETIME) ? new self() : null;
    }

    public function fromParquetType(mixed $data): DateTimeImmutable
    {
        $micros = $this->stored->fromParquetType($data);
        $seconds = floor_div($micros, 1_000_000);

        $dateTime = DateTimeImmutable::createFromFormat('U.u', sprintf(
            '%d.%06d',
            $seconds,
            $micros - ($seconds * 1_000_000),
        ));

        if ($dateTime === false) {
            throw new RuntimeException('Failed to convert INT96 to DateTime, given: ' . $micros);
        }

        return $dateTime;
    }

    /**
     * @return array<never>
     */
    public function toParquetType(mixed $data): array
    {
        throw new RuntimeException(
            "Converting DateTime to INT96 is deprecated and should not be used, please use INT64 to store \DateTime objects as number of microseconds since Jan 1 1970.",
        );
    }
}
