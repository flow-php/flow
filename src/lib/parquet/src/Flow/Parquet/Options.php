<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\SizeUnits;
use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Writer\StatisticsLevel;
use UnitEnum;

use function array_key_exists;
use function array_map;
use function constant;
use function get_debug_type;
use function gettype;
use function is_a;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_scalar;
use function is_string;

/**
 * @type ParquetOptions = array<string, mixed> every option keyed by its Option case name, enums as {enum, case}
 */
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
            Option::ROW_GROUP_MAXIMUM_ROWS_COUNT->name => 1_048_576,
            Option::ROW_GROUP_SIZE_BYTES->name => SizeUnits::MiB_SIZE * 32,
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL->name => 1000,
            Option::STATISTICS_LEVEL->name => StatisticsLevel::PAGE,
            Option::STATISTICS_TRUNCATE_LENGTH->name => 64,
            Option::COLUMN_INDEX_TRUNCATE_LENGTH->name => 64,
            Option::PAGE_HEADER_STATISTICS->name => false,
            Option::DICTIONARY_ENABLED->name => true,
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
     * @param array<array-key, mixed> $data the shape of normalize(): every key an Option case name
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $data): self
    {
        $restore = static function (mixed $value) use (&$restore): mixed {
            if ($value === null || is_scalar($value)) {
                return $value;
            }

            if (!is_array($value)) {
                throw new InvalidArgumentException(
                    'Option values must be scalars, null, enums or arrays of them, got: ' . get_debug_type($value),
                );
            }

            if (array_key_exists('enum', $value) && array_key_exists('case', $value)) {
                if (
                    !is_string($value['enum'])
                    || !is_a($value['enum'], UnitEnum::class, true)
                    || !is_string($value['case'])
                ) {
                    throw new InvalidArgumentException(
                        'Option enum values must name an enum class and one of its cases',
                    );
                }

                // @mago-ignore analysis:mixed-assignment
                $enum = constant($value['enum'] . '::' . $value['case']);

                if (!$enum instanceof UnitEnum) {
                    throw new InvalidArgumentException(
                        'Option enum values must name an enum class and one of its cases',
                    );
                }

                return $enum;
            }

            if (array_key_exists('bloom_filter', $value) && is_array($value['bloom_filter'])) {
                // @mago-ignore analysis:mixed-assignment
                $fpp = $value['bloom_filter']['fpp'] ?? null;
                // @mago-ignore analysis:mixed-assignment
                $ndv = $value['bloom_filter']['ndv'] ?? null;

                if (!is_float($fpp) && !is_int($fpp)) {
                    throw new InvalidArgumentException('A bloom filter option must hold a numeric "fpp"');
                }

                if ($ndv !== null && !is_int($ndv)) {
                    throw new InvalidArgumentException('A bloom filter option "ndv" must be an integer or null');
                }

                return new BloomFilterOptions((float) $fpp, $ndv);
            }

            return array_map($restore, $value);
        };

        $options = new self();

        // @mago-ignore analysis:mixed-assignment
        foreach ($data as $name => $value) {
            $option = null;

            foreach (Option::cases() as $case) {
                if ($case->name === $name) {
                    $option = $case;
                }
            }

            if ($option === null) {
                throw new InvalidArgumentException("Unknown parquet option: {$name}");
            }

            $restored = $restore($value);

            if ($restored instanceof BloomFilterOptions) {
                throw new InvalidArgumentException("Option {$name} must not be a bare bloom filter option");
            }

            $options->set($option, $restored);
        }

        return $options;
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
     * Every option keyed by its case name; enums as {enum, case}, bloom filter options as {bloom_filter: {fpp, ndv}}.
     *
     * @return ParquetOptions
     */
    public function normalize(): array
    {
        $normalize = static function (mixed $value) use (&$normalize): mixed {
            if ($value instanceof UnitEnum) {
                return ['enum' => $value::class, 'case' => $value->name];
            }

            if ($value instanceof BloomFilterOptions) {
                return ['bloom_filter' => ['fpp' => $value->fpp, 'ndv' => $value->ndv]];
            }

            if (is_array($value)) {
                return array_map($normalize, $value);
            }

            return $value;
        };

        return array_map($normalize, $this->options);
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
