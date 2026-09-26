<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use InvalidArgumentException;

use function abs;
use function Flow\Parquet\floor_div;
use function get_debug_type;
use function is_int;
use function sprintf;

final readonly class Int32DateConverter implements Converter
{
    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        if ($column->type() === PhysicalType::INT32 && $column->logicalType()?->name() === LogicalType::DATE) {
            return new self();
        }

        if ($column->type() === PhysicalType::INT32 && $column->convertedType() === ConvertedType::DATE) {
            return new self();
        }

        return null;
    }

    public function fromParquetType(mixed $data): DateTimeImmutable
    {
        if (!is_int($data)) {
            throw new InvalidArgumentException(sprintf('Expected int, got %s', get_debug_type($data)));
        }

        return $this->numberOfDaysToDateTime($data);
    }

    public function toParquetType(mixed $data): int
    {
        if (!$data instanceof DateTime && !$data instanceof DateTimeImmutable) {
            throw new InvalidArgumentException(sprintf(
                'Expected DateTime or DateTimeImmutable, got %s',
                get_debug_type($data),
            ));
        }

        return floor_div($data->getTimestamp() + $data->getOffset(), 86_400);
    }

    private function numberOfDaysToDateTime(int $data): DateTimeImmutable
    {
        $epoch = new DateTimeImmutable('1970-01-01 00:00:00 UTC');

        if ($data >= 0) {
            return $epoch->add(new DateInterval('P' . $data . 'D'));
        }

        return $epoch->sub(new DateInterval('P' . abs($data) . 'D'));
    }
}
