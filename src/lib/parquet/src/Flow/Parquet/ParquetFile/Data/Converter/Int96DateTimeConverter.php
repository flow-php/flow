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

use function array_values;
use function bin2hex;
use function Flow\Parquet\floor_div;
use function sprintf;
use function unpack;

final readonly class Int96DateTimeConverter implements Converter
{
    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        return $column->type() === PhysicalType::INT96 && $options->get(Option::INT_96_AS_DATETIME) ? new self() : null;
    }

    public function fromParquetType(mixed $data): DateTimeImmutable
    {
        /** @var string $data */
        return $this->convertRawBytesToDateTime($data);
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

    private function convertRawBytesToDateTime(string $bytes): DateTimeImmutable
    {
        $unpacked = unpack('C*', $bytes);

        if ($unpacked === false) {
            throw new RuntimeException('Failed to unpack INT96 bytes: ' . bin2hex($bytes));
        }

        /** @var array<int, int> $bytesArray */
        $bytesArray = array_values($unpacked);
        $daysInEpoch = $bytesArray[8] | ($bytesArray[9] << 8) | ($bytesArray[10] << 16) | ($bytesArray[11] << 24);

        // Convert the first 8 bytes to the number of nanoseconds within the day
        $nanosecondsWithinDay =
            $bytesArray[0]
            | ($bytesArray[1] << 8)
            | ($bytesArray[2] << 16)
            | ($bytesArray[3] << 24)
            | ($bytesArray[4] << 32)
            | ($bytesArray[5] << 40)
            | ($bytesArray[6] << 48)
            | ($bytesArray[7] << 56);

        $micros = (($daysInEpoch - 2_440_588) * 86_400_000_000) + floor_div($nanosecondsWithinDay, 1_000);
        $seconds = floor_div($micros, 1_000_000);

        $dateTime = DateTimeImmutable::createFromFormat('U.u', sprintf(
            '%d.%06d',
            $seconds,
            $micros - ($seconds * 1_000_000),
        ));

        if ($dateTime === false) {
            throw new RuntimeException('Failed to convert INT96 to DateTime, given bytes: ' . bin2hex($bytes));
        }

        return $dateTime;
    }
}
