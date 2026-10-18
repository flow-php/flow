<?php

declare(strict_types=1);

namespace Flow\Parquet\BloomFilter;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;

use function get_debug_type;

final readonly class BloomFilterOptions
{
    /**
     * arrow-rs DEFAULT_MAX_ROW_GROUP_ROW_COUNT, its default ndv.
     */
    public const int DEFAULT_NDV = 1_048_576;

    /**
     * @param null|int $ndv the distinct values a row group is expected to hold, null for DEFAULT_NDV
     */
    public function __construct(
        public float $fpp = 0.05,
        public ?int $ndv = null,
    ) {
        if (!($fpp > 0.0 && $fpp < 1.0)) {
            throw new InvalidArgumentException('A bloom filter false positive probability must be between 0 and 1, got '
            . $fpp);
        }

        if ($ndv !== null && $ndv < 1) {
            throw new InvalidArgumentException('A bloom filter ndv must be at least 1, got ' . $ndv);
        }
    }

    /**
     * Option::COLUMNS_BLOOM_FILTERS, refused when a value is not a BloomFilterOptions.
     *
     * @return array<string, self>
     */
    public static function forColumns(Options $options): array
    {
        $columns = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($options->getArray(Option::COLUMNS_BLOOM_FILTERS) ?? [] as $path => $bloomFilter) {
            if (!$bloomFilter instanceof self) {
                throw new InvalidArgumentException(
                    'Option COLUMNS_BLOOM_FILTERS takes BloomFilterOptions, got '
                    . get_debug_type($bloomFilter)
                    . ' for column "'
                    . $path
                    . '"',
                );
            }

            $columns[(string) $path] = $bloomFilter;
        }

        return $columns;
    }
}
