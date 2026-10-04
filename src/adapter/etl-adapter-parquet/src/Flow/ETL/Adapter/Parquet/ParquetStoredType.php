<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function array_key_exists;
use function is_array;

/**
 * Parquet TIMESTAMP stores the instant, never the zone: two types equal here write the same file.
 */
final class ParquetStoredType
{
    /**
     * @param array<array-key, mixed> $normalized Type::normalize()
     *
     * @return array<array-key, mixed>
     */
    public static function of(array $normalized): array
    {
        if (array_key_exists('type', $normalized) && $normalized['type'] === 'datetime') {
            unset($normalized['zone']);
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($normalized as $key => $value) {
            if (is_array($value)) {
                $normalized[$key] = self::of($value);
            }
        }

        return $normalized;
    }
}
