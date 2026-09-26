<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use DateInterval;
use DateTimeInterface;

use function array_map;

final class TemporalValues
{
    /**
     * @param iterable<array<array-key, mixed>> $rows
     *
     * @return list<array<array-key, mixed>>
     */
    public static function format(iterable $rows): array
    {
        $formatted = [];

        foreach ($rows as $row) {
            $formatted[] = array_map(static fn(mixed $value): mixed => match (true) {
                $value instanceof DateTimeInterface => $value->format('Y-m-d H:i:s.u P'),
                $value instanceof DateInterval => $value->format('%H:%I:%S.%F'),
                default => $value,
            }, $row);
        }

        return $formatted;
    }
}
