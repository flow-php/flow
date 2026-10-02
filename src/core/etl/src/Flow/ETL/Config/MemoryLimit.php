<?php

declare(strict_types=1);

namespace Flow\ETL\Config;

use Flow\ETL\Dataset\Memory\Configuration;
use Flow\ETL\Dataset\Memory\Unit;

use function getenv;
use function is_string;

/**
 * The process memory a spilling algorithm (sort, TopN, join build) lets the whole process reach before it spills - the
 * limit is compared with the process's usage, never with the algorithm's own buffer, so a sort and a join in one
 * pipeline together stay under it. FLOW_MAX_MEMORY when set, otherwise 70% of PHP's memory_limit, and 1 GiB when PHP's
 * memory_limit is -1 - an unlimited algorithm would grow until the OS kills PHP.
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

        return (
            (new Configuration(100 - self::PERCENTAGE_OF_MEMORY_LIMIT))->limit() ?? Unit::fromGb(
                self::WITHOUT_MEMORY_LIMIT_GB,
            )
        );
    }
}
