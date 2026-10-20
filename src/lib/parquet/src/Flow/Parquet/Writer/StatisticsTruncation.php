<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function chr;
use function max;
use function ord;
use function preg_match;
use function strlen;
use function substr;

/**
 * arrow-rs 60 column/writer/mod.rs truncate_min_value / truncate_max_value. A truncated bound is not exact; a bound
 * that cannot be truncated (or whose increment fails) is returned as given.
 */
final readonly class StatisticsTruncation
{
    public function __construct(
        private int $length,
        Option $option = Option::STATISTICS_TRUNCATE_LENGTH,
    ) {
        if ($length < 1) {
            throw new InvalidArgumentException($option->name . ' must be greater than 0, ' . $length . ' given');
        }
    }

    public function max(FlatColumn $column, string $bytes): string
    {
        if (!$this->truncates($column, $bytes)) {
            return $bytes;
        }

        if ($this->isUtf8($column, $bytes)) {
            $split = $this->lastCharBoundary($bytes, max(0, $this->length - 3), $this->length);

            return $split === null ? $bytes : $this->incrementUtf8(substr($bytes, 0, $split)) ?? $bytes;
        }

        return $this->increment(substr($bytes, 0, $this->length)) ?? $bytes;
    }

    public function min(FlatColumn $column, string $bytes): string
    {
        if (!$this->truncates($column, $bytes)) {
            return $bytes;
        }

        if ($this->isUtf8($column, $bytes)) {
            $split = $this->lastCharBoundary($bytes, 1, $this->length);

            return $split === null ? $bytes : substr($bytes, 0, $split);
        }

        return substr($bytes, 0, $this->length);
    }

    private function encodeCodePoint(int $codePoint): string
    {
        return match (true) {
            $codePoint < 0x80 => chr($codePoint),
            $codePoint < 0x800 => chr(0xC0 | ($codePoint >> 6)) . chr(0x80 | ($codePoint & 0x3F)),
            $codePoint < 0x10000 => chr(0xE0 | ($codePoint >> 12))
                . chr(0x80 | (($codePoint >> 6) & 0x3F))
                . chr(0x80 | ($codePoint & 0x3F)),
            default => chr(0xF0 | ($codePoint >> 18))
                . chr(0x80 | (($codePoint >> 12) & 0x3F))
                . chr(0x80 | (($codePoint >> 6) & 0x3F))
                . chr(0x80 | ($codePoint & 0x3F)),
        };
    }

    private function increment(string $bytes): ?string
    {
        for ($i = strlen($bytes) - 1; $i >= 0; $i--) {
            $byte = ord($bytes[$i]);
            $bytes[$i] = chr(($byte + 1) & 0xFF);

            if ($byte !== 0xFF) {
                return $bytes;
            }
        }

        return null;
    }

    /**
     * Increments the last code point that stays a valid code point of the same byte width, dropping the ones after it.
     */
    private function incrementUtf8(string $utf8): ?string
    {
        $end = strlen($utf8);

        while ($end > 0) {
            $start = $end - 1;

            while ($start > 0 && (ord($utf8[$start]) & 0xC0) === 0x80) {
                $start--;
            }

            $width = $end - $start;
            $lead = ord($utf8[$start]);
            $codePoint = match ($width) {
                1 => $lead,
                2 => $lead & 0x1F,
                3 => $lead & 0x0F,
                default => $lead & 0x07,
            };

            for ($i = $start + 1; $i < $end; $i++) {
                $codePoint = ($codePoint << 6) | (ord($utf8[$i]) & 0x3F);
            }

            $next = $codePoint + 1;
            $nextWidth = match (true) {
                $next < 0x80 => 1,
                $next < 0x800 => 2,
                $next < 0x10000 => 3,
                default => 4,
            };

            if (($next < 0xD800 || $next > 0xDFFF) && $next <= 0x10FFFF && $nextWidth === $width) {
                return substr($utf8, 0, $start) . $this->encodeCodePoint($next);
            }

            $end = $start;
        }

        return null;
    }

    private function isUtf8(FlatColumn $column, string $bytes): bool
    {
        return (
            (
                $column->logicalType()?->is(LogicalType::STRING) === true
                || $column->convertedType() === ConvertedType::UTF8
            )
            && preg_match('//u', $bytes) === 1
        );
    }

    private function lastCharBoundary(string $utf8, int $from, int $to): ?int
    {
        for ($split = $to; $split >= $from; $split--) {
            if ($split === 0 || $split === strlen($utf8) || (ord($utf8[$split]) & 0xC0) !== 0x80) {
                return $split;
            }
        }

        return null;
    }

    private function truncates(FlatColumn $column, string $bytes): bool
    {
        if (strlen($bytes) <= $this->length) {
            return false;
        }

        return match ($column->type()) {
            PhysicalType::BYTE_ARRAY => true,
            PhysicalType::FIXED_LEN_BYTE_ARRAY => $column->logicalType()?->is(LogicalType::DECIMAL) !== true
                && $column->logicalType()?->is(LogicalType::FLOAT16) !== true,
            default => false,
        };
    }
}
