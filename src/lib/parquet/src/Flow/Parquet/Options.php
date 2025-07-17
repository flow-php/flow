<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\SizeUnits;
use Flow\Parquet\Exception\InvalidArgumentException;

final class Options
{
    /**
     * @var array<string, null|array<mixed>|bool|float|int>
     */
    private array $options;

    public function __construct()
    {
        $this->options = [
            Option::BYTE_ARRAY_TO_STRING->name => true,
            Option::ROUND_NANOSECONDS->name => false,
            Option::INT_96_AS_DATETIME->name => true,
            Option::PAGE_MAXIMUM_ROWS_COUNT->name => 1000,
            Option::PAGE_SIZE_BYTES->name => SizeUnits::KiB_SIZE * 128,
            Option::PAGE_SIZE_CHECK_INTERVAL->name => 100,
            Option::ROW_GROUP_SIZE_BYTES->name => SizeUnits::MiB_SIZE * 32,
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL->name => 1000,
            Option::DICTIONARY_PAGE_SIZE->name => SizeUnits::MiB_SIZE,
            Option::DICTIONARY_PAGE_MIN_CARDINALITY_RATION->name => 0.4,
            Option::BROTLI_COMPRESSION_LEVEL->name => 11,
            Option::GZIP_COMPRESSION_LEVEL->name => 9,
            Option::LZ4_COMPRESSION_LEVEL->name => 4,
            Option::ZSTD_COMPRESSION_LEVEL->name => 3,
            Option::WRITER_VERSION->name => 1,
            Option::VALIDATE_DATA->name => true,
            Option::COLUMNS_ENCODINGS->name => null,
            Option::COLUMNS_COMPRESSIONS->name => null,
        ];
    }

    public static function default() : self
    {
        return new self;
    }

    /**
     * @return null|array<mixed>|bool|float|int
     */
    public function get(Option $option) : bool|int|float|array|null
    {
        return $this->options[$option->name];
    }

    /**
     * @return null|array<mixed>
     */
    public function getArray(Option $option) : ?array
    {
        $value = $this->options[$option->name] ?? null;

        if ($value === null) {
            return null;
        }

        if (\is_array($value)) {
            return $value;
        }

        throw new InvalidArgumentException("Option {$option->name} is not an array, but: " . \gettype($value));
    }

    public function getBool(Option $option) : bool
    {
        $value = $this->options[$option->name];

        if (!\is_bool($value)) {
            throw new InvalidArgumentException("Option {$option->name} is not a boolean, but: " . \gettype($value));
        }

        return $value;
    }

    public function getInt(Option $option) : int
    {
        $value = $this->options[$option->name];

        if (!\is_int($value)) {
            throw new InvalidArgumentException("Option {$option->name} is not an integer, but: " . \gettype($value));
        }

        return $value;
    }

    public function has(Option $option) : bool
    {
        $value = $this->options[$option->name] ?? null;

        return $value !== null;
    }

    /**
     * @param null|array<mixed>|bool|float|int $value
     */
    public function set(Option $option, bool|int|float|array|null $value) : self
    {
        $this->options[$option->name] = $value;

        return $this;
    }
}
