<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Dremel\Dremel;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Page\ColumnData;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class DataBuilder
{
    public function __construct(
        private readonly Options $options,
        private readonly LoggerInterface $logger = new NullLogger()
    ) {
    }

    /**
     * @psalm-suppress MixedArrayOffset
     * @psalm-suppress MixedAssignment
     * @psalm-suppress MixedArgumentTypeCoercion
     */
    public function build(ColumnData $columnData, Column $column) : \Generator
    {
        $dremel = new Dremel($this->logger);

        if (!$column->isList() && $column->isMap()) {
            throw new RuntimeException('Flat data builder supports only flat column types, not LIST and MAP.');
        }

        foreach ($dremel->assemble($columnData->repetitions, $columnData->definitions, $columnData->values) as $value) {
            yield $this->enrichData($value, $column);
        }
    }

    /**
     * @TODO : This should be optimized by moving it into different class, checking column only once, caching it and using dedicated data transformer for values to improve performance
     *
     * @psalm-suppress PossiblyFalseReference
     * @psalm-suppress MixedArgument
     * @psalm-suppress MixedAssignment
     * @psalm-suppress MixedOperand
     * @psalm-suppress MixedReturnStatement
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress PossiblyNullOperand
     */
    private function enrichData(mixed $value, Column $column) : mixed
    {
        if ($column instanceof NestedColumn) {
            return $value;
        }

        /** @var FlatColumn $column */
        if ($value === null) {
            return null;
        }

        if ($column->type() === PhysicalType::INT96 && $this->options->get(Option::INT_96_AS_DATETIME)) {
            if (\is_array($value) && \count($value) && !\is_array($value[0])) {
                /** @psalm-suppress MixedArgumentTypeCoercion */
                return $this->nanoToDateTimeImmutable($value);
            }

            if (\is_array($value)) {
                $enriched = [];

                /** @var array<int> $val */
                foreach ($value as $val) {
                    $enriched[] = $this->nanoToDateTimeImmutable($val);
                }

                return $enriched;
            }

            return $value;
        }

        if ($column->logicalType()?->name() === 'DECIMAL') {
            if ($column->scale() === null || $column->precision() === null) {
                return $value;
            }
            $divisor = $column->precision() ** $column->scale();

            if (\is_scalar($value)) {
                return ((int) $value) / $divisor;
            }

            if (\is_array($value)) {
                $enriched = [];

                foreach ($value as $val) {
                    $enriched[] = $val / $divisor;
                }

                return $enriched;
            }
        }

        if ($column->logicalType()?->name() === 'TIMESTAMP') {
            /** @phpstan-ignore-next-line  */
            if ($column->logicalType()?->timestampData()?->nanos() && $this->options->get(Option::ROUND_NANOSECONDS) === false) {
                return $value;
            }

            if (!\is_int($value) && !\is_array($value)) {
                return $value;
            }

            /** @phpstan-ignore-next-line  */
            $isMillis = (bool) $column->logicalType()?->timestampData()?->millis();
            /** @phpstan-ignore-next-line  */
            $isMicros = ($column->logicalType()?->timestampData()?->micros() || $column->logicalType()?->timestampData()?->nanos());

            $convertValue = static function (int $val) use ($isMillis, $isMicros) : \DateTimeImmutable|int {
                if ($isMillis) {
                    $seconds = (int) ($val / 1000);
                    $fraction = \str_pad((string) ($val % 1000), 3, '0', STR_PAD_LEFT) . '000';  // Pad milliseconds to microseconds
                } elseif ($isMicros) {
                    $seconds = (int) ($val / 1000000);
                    $fraction = \str_pad((string) ($val % 1000000), 6, '0', STR_PAD_LEFT);
                } else {
                    return $val;
                }

                $datetime = \DateTimeImmutable::createFromFormat('U.u', \sprintf('%d.%s', $seconds, $fraction));

                if (!$datetime) {
                    return $val;
                }

                return $datetime;
            };

            if (\is_scalar($value)) {
                return $convertValue($value);
            }

            $enriched = [];

            foreach ($value as $val) {
                $enriched[] = $convertValue($val);
            }

            return $enriched;
        }

        if ($column->logicalType()?->name() === 'DATE') {
            if (\is_int($value)) {
                /** @phpstan-ignore-next-line  */
                return \DateTimeImmutable::createFromFormat('Y-m-d', '1970-01-01')
                    ->modify(\sprintf('+%d days', $value))
                    ->setTime(0, 0, 0, 0);
            }

            if (\is_array($value)) {
                $enriched = [];

                foreach ($value as $val) {
                    /** @phpstan-ignore-next-line  */
                    $enriched[] = \DateTimeImmutable::createFromFormat('Y-m-d', '1970-01-01')
                        ->modify(\sprintf('+%d days', $val))
                        ->setTime(0, 0, 0, 0);
                }

                return $enriched;
            }
        }

        return $value;
    }

    /**
     * @psalm-suppress FalsableReturnStatement
     * @psalm-suppress InvalidFalsableReturnType
     *
     * @param array<array-key, int> $bytes
     */
    private function nanoToDateTimeImmutable(array $bytes) : \DateTimeImmutable
    {
        $daysInEpoch = $bytes[8] | ($bytes[9] << 8) | ($bytes[10] << 16) | ($bytes[11] << 24);

        // Convert the first 8 bytes to the number of nanoseconds within the day
        $nanosecondsWithinDay = $bytes[0] | ($bytes[1] << 8) | ($bytes[2] << 16) | ($bytes[3] << 24) |
            ($bytes[4] << 32) | ($bytes[5] << 40) | ($bytes[6] << 48) | ($bytes[7] << 56);

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

        /** @phpstan-ignore-next-line */
        return \DateTimeImmutable::createFromFormat('U.u', \sprintf('%d.%06d', $seconds, $microseconds));
    }
}
