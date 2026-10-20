<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\SizeUnits;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Writer\StatisticsLevel;
use UnitEnum;

use function get_debug_type;
use function gettype;
use function is_array;
use function is_bool;
use function is_int;
use function is_string;

final class Options
{
    /**
     * @var array<string, null|array<mixed>|bool|float|int|string|UnitEnum>
     */
    private array $options;

    public function __construct()
    {
        $this->options = [
            Option::ARROW_WRITE_BATCH_SIZE->name => 1000,
            Option::INT_96_AS_DATETIME->name => true,
            Option::PAGE_MAXIMUM_ROWS_COUNT->name => 1000,
            Option::PAGE_SIZE_BYTES->name => SizeUnits::KiB_SIZE * 128,
            Option::PAGE_SIZE_CHECK_INTERVAL->name => 100,
            Option::ROW_GROUP_SIZE_BYTES->name => SizeUnits::MiB_SIZE * 32,
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL->name => 1000,
            Option::STATISTICS_LEVEL->name => StatisticsLevel::PAGE,
            Option::STATISTICS_TRUNCATE_LENGTH->name => 64,
            Option::COLUMN_INDEX_TRUNCATE_LENGTH->name => 64,
            Option::PAGE_HEADER_STATISTICS->name => false,
            Option::DICTIONARY_PAGE_SIZE->name => SizeUnits::MiB_SIZE,
            Option::BROTLI_COMPRESSION_LEVEL->name => 11,
            Option::GZIP_COMPRESSION_LEVEL->name => 9,
            Option::LZ4_COMPRESSION_LEVEL->name => 4,
            Option::ZSTD_COMPRESSION_LEVEL->name => 3,
            Option::WRITER_VERSION->name => 1,
            Option::VALIDATE_DATA->name => true,
            Option::COLUMNS_ENCODINGS->name => null,
            Option::COLUMNS_COMPRESSIONS->name => null,
            Option::COLUMNS_BLOOM_FILTERS->name => null,
        ];
    }

    public static function default(): self
    {
        return new self();
    }

    /**
     * @return null|array<mixed>|bool|float|int|string|UnitEnum
     */
    public function get(Option $option): bool|int|float|string|array|UnitEnum|null
    {
        return $this->options[$option->name];
    }

    /**
     * @return null|array<mixed>
     */
    public function getArray(Option $option): ?array
    {
        $value = $this->options[$option->name] ?? null;

        if ($value === null) {
            return null;
        }

        if (is_array($value)) {
            return $value;
        }

        throw new InvalidArgumentException("Option {$option->name} is not an array, but: " . gettype($value));
    }

    public function getBool(Option $option): bool
    {
        $value = $this->options[$option->name];

        if (!is_bool($value)) {
            throw new InvalidArgumentException("Option {$option->name} is not a boolean, but: " . gettype($value));
        }

        return $value;
    }

    /**
     * @template T of UnitEnum
     *
     * @param class-string<T> $class
     *
     * @return T
     */
    public function getEnum(Option $option, string $class): UnitEnum
    {
        $value = $this->options[$option->name];

        if (!$value instanceof $class) {
            throw new InvalidArgumentException(
                "Option {$option->name} is not a {$class}, but: " . get_debug_type($value),
            );
        }

        return $value;
    }

    public function getInt(Option $option): int
    {
        $value = $this->options[$option->name];

        if (!is_int($value)) {
            throw new InvalidArgumentException("Option {$option->name} is not an integer, but: " . gettype($value));
        }

        return $value;
    }

    public function getString(Option $option): string
    {
        $value = $this->options[$option->name];

        if (!is_string($value)) {
            throw new InvalidArgumentException("Option {$option->name} is not a string, but: " . gettype($value));
        }

        return $value;
    }

    public function has(Option $option): bool
    {
        $value = $this->options[$option->name] ?? null;

        return $value !== null;
    }

    /**
     * @param null|array<mixed>|bool|float|int|string|UnitEnum $value
     */
    public function set(Option $option, bool|int|float|string|array|UnitEnum|null $value): self
    {
        $this->options[$option->name] = $value;

        return $this;
    }
}
