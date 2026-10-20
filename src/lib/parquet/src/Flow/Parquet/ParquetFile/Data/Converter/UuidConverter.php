<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Stringable;

use function bin2hex;
use function ctype_xdigit;
use function get_debug_type;
use function hex2bin;
use function is_object;
use function is_string;
use function method_exists;
use function sprintf;
use function str_replace;
use function strlen;
use function substr;

final readonly class UuidConverter implements Converter
{
    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        return $column->logicalType()?->name() === LogicalType::UUID ? new self() : null;
    }

    public function fromParquetType(mixed $data): string
    {
        if (!is_string($data) || strlen($data) !== 16) {
            throw new RuntimeException('UUID must be read as 16 bytes from Parquet file');
        }

        $hex = bin2hex($data);

        return sprintf(
            '%s-%s-%s-%s-%s',
            substr($hex, 0, 8),
            substr($hex, 8, 4),
            substr($hex, 12, 4),
            substr($hex, 16, 4),
            substr($hex, 20, 12),
        );
    }

    public function toParquetType(mixed $data): string
    {
        $uuid = match (true) {
            is_string($data) => $data,
            is_object($data) && method_exists($data, 'toString') => is_string($data->toString())
                ? (string) $data->toString()
                : throw new RuntimeException(sprintf(
                    'UUID toString() must return string, got %s',
                    get_debug_type($data->toString()),
                )),
            $data instanceof Stringable => (string) $data,
            default => throw new RuntimeException('UUID must be written as a string or Stringable object'),
        };

        $hex = str_replace('-', '', $uuid);

        if (strlen($hex) !== 32 || !ctype_xdigit($hex)) {
            throw new RuntimeException('Invalid UUID format: ' . $uuid);
        }

        return (string) hex2bin($hex);
    }
}
