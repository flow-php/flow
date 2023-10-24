<?php declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use Flow\Parquet\BinaryReader\Bytes;
use Flow\Parquet\Data\Converter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class Int96DateTimeConverter implements Converter
{
    public function fromParquetType(mixed $data) : \DateTimeImmutable
    {
        return $this->convertArrayOfBytesToDateTime($data);
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ($column->type() === PhysicalType::INT96 && $options->get(Option::INT_96_AS_DATETIME)) {
            return true;
        }

        return false;
    }

    public function toParquetType(mixed $data) : array
    {
        throw new RuntimeException("Converting DateTime to INT96 is deprecated and should not be used, please use INT64 to store \DateTime objects as number of microseconds since Jan 1 1970.");
    }

    private function convertArrayOfBytesToDateTime(Bytes $bytes) : \DateTimeImmutable
    {
        $bytesArray = $bytes->toArray();
        $daysInEpoch = $bytesArray[8] | ($bytesArray[9] << 8) | ($bytesArray[10] << 16) | ($bytesArray[11] << 24);

        // Convert the first 8 bytes to the number of nanoseconds within the day
        $nanosecondsWithinDay = $bytesArray[0] | ($bytesArray[1] << 8) | ($bytesArray[2] << 16) | ($bytesArray[3] << 24) |
            ($bytesArray[4] << 32) | ($bytesArray[5] << 40) | ($bytesArray[6] << 48) | ($bytesArray[7] << 56);

        // The Julian epoch starts on January 1, 4713 BCE.
        // The Unix epoch starts on January 1, 1970 CE.
        // The number of days between these two dates is 2440588.
        $daysSinceUnixEpoch = $daysInEpoch - 2440588;

        // Convert the days since the Unix epoch and the nanoseconds within the day to a Unix timestamp
        $timestampSeconds = $daysSinceUnixEpoch * 86400 + $nanosecondsWithinDay / 1e9;

        // Separate the seconds and fractional seconds parts of the timestamp
        $seconds = \floor($timestampSeconds);
        $fraction = $timestampSeconds - $seconds;

        // Convert the fractional seconds to milliseconds
        $microseconds = \round($fraction * 1e6);

        $dateTime = \DateTimeImmutable::createFromFormat('U.u', \sprintf('%d.%06d', $seconds, $microseconds));

        if ($dateTime === false) {
            throw new RuntimeException('Failed to convert INT96 to DateTime, given bytes: ' . \json_encode($bytesArray, JSON_THROW_ON_ERROR));
        }

        return $dateTime;
    }
}
