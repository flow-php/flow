<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime;

use Flow\ETL\Values\Memory\Unit;

use function getenv;
use function ini_get;
use function is_string;
use function str_starts_with;

/**
 * Compared with process usage; FLOW_MAX_MEMORY, else 70% of memory_limit, else 1 GiB.
 */
final readonly class MemoryLimit
{
    public const int PERCENTAGE_OF_MEMORY_LIMIT = 70;

    public const string ENV = 'FLOW_MAX_MEMORY';

    public const int WITHOUT_MEMORY_LIMIT_GB = 1;

    public static function default(): Unit
    {
        $value = getenv(self::ENV);

        if (is_string($value) && $value !== '') {
            return Unit::fromString($value);
        }

        $limit = ini_get('memory_limit');

        if ($limit === false || str_starts_with($limit, '-')) {
            return Unit::fromGb(self::WITHOUT_MEMORY_LIMIT_GB);
        }

        return Unit::fromString($limit)->percentage(self::PERCENTAGE_OF_MEMORY_LIMIT);
    }
}
