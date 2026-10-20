<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Integer;

use function bcsub;
use function ctype_digit;
use function get_debug_type;
use function is_int;
use function is_string;
use function sprintf;
use function strcmp;
use function strlen;

/**
 * UINT_64: the physical value is the INT64 bit pattern, the logical value its canonical decimal digit string.
 */
final readonly class UnsignedInt64Converter implements Converter
{
    public const string MAX = '18446744073709551615';

    private const string TWO_POW_64 = '18446744073709551616';

    private const string PHP_INT_MAX = '9223372036854775807';

    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        return Integer::forColumn($column)?->is(64, false) === true ? new self() : null;
    }

    public function fromParquetType(mixed $data): string
    {
        if (!is_int($data)) {
            throw new InvalidArgumentException(sprintf('Expected int, got %s', get_debug_type($data)));
        }

        return sprintf('%u', $data);
    }

    /**
     * An int >= 0, or a canonical digit string (no sign, no leading zero) up to 18446744073709551615.
     *
     * @assert-if-true non-negative-int|numeric-string $data
     */
    public static function accepts(mixed $data): bool
    {
        if (is_int($data)) {
            return $data >= 0;
        }

        return (
            is_string($data)
            && ctype_digit($data)
            && (strlen($data) === 1 || $data[0] !== '0')
            && !self::exceeds($data, self::MAX)
        );
    }

    public function toParquetType(mixed $data): int
    {
        if (is_int($data) && $data >= 0) {
            return $data;
        }

        if (!self::accepts($data)) {
            throw new InvalidArgumentException(sprintf(
                'Expected an int >= 0 or a digit string up to %s, got %s',
                self::MAX,
                is_int($data) || is_string($data) ? $data : get_debug_type($data),
            ));
        }

        return self::exceeds($data, self::PHP_INT_MAX) ? (int) bcsub($data, self::TWO_POW_64) : (int) $data;
    }

    private static function exceeds(string $digits, string $limit): bool
    {
        return strlen($digits) > strlen($limit) || strlen($digits) === strlen($limit) && strcmp($digits, $limit) > 0;
    }
}
