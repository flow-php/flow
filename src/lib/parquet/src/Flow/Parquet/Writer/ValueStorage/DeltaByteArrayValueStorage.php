<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ValueStorage;

use Flow\Parquet\Data\DeltaByteArrayEncoder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function array_filter;
use function array_push;
use function array_values;
use function count;
use function gettype;
use function implode;
use function in_array;
use function is_null;
use function is_string;
use function sprintf;
use function strlen;

final class DeltaByteArrayValueStorage implements ValueStorage
{
    private int $size = 0;

    /**
     * @var list<string>
     */
    private array $values = [];

    public function __construct(
        private readonly DeltaByteArrayEncoder $encoder = new DeltaByteArrayEncoder(),
    ) {}

    public function addValues(FlatColumn $column, array $values): void
    {
        if (!in_array($column->type(), [PhysicalType::BYTE_ARRAY, PhysicalType::FIXED_LEN_BYTE_ARRAY], true)) {
            throw new InvalidArgumentException(
                'DELTA_BYTE_ARRAY encoding only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY physical types',
            );
        }

        $strings = array_filter($values, is_string(...));

        if ((count($strings) + count(array_filter($values, is_null(...)))) !== count($values)) {
            throw new InvalidArgumentException(sprintf(
                'DELTA_BYTE_ARRAY encoding requires string values, got %s',
                gettype(
                    array_values(array_filter(
                        $values,
                        static fn(mixed $value): bool => $value !== null && !is_string($value),
                    ))[0],
                ),
            ));
        }

        // PLAIN prefixes every BYTE_ARRAY value with its 4-byte length, FIXED_LEN_BYTE_ARRAY values with nothing
        $lengthPrefix = $column->type() === PhysicalType::BYTE_ARRAY ? 4 : 0;

        array_push($this->values, ...array_values($strings));
        $this->size += (count($strings) * $lengthPrefix) + strlen(implode('', $strings));
    }

    public function encoding(): Encodings
    {
        return Encodings::DELTA_BYTE_ARRAY;
    }

    public function getBuffer(): string
    {
        return $this->encoder->encode($this->values);
    }

    public function isEmpty(): bool
    {
        return $this->values === [];
    }

    public function reset(): void
    {
        $this->values = [];
        $this->size = 0;
    }

    public function plainSize(): int
    {
        return $this->size;
    }
}
