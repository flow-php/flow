<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Memory;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function getenv;
use function ini_get;
use function is_string;
use function sprintf;
use function str_starts_with;

/**
 * Operation default limit and the process cap; read in the running process.
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

    /**
     * @throws InvalidArgumentException
     */
    public static function processCap(): ?Unit
    {
        $limit = ini_get('memory_limit');

        if ($limit === false || str_starts_with($limit, '-')) {
            return null;
        }

        $value = getenv(self::ENV);

        if (!is_string($value) || $value === '') {
            return Unit::fromString($limit)->percentage(self::PERCENTAGE_OF_MEMORY_LIMIT);
        }

        $cap = Unit::fromString($value);

        if ($cap->isGreaterThan(Unit::fromString($limit))) {
            throw new InvalidArgumentException(sprintf(
                '%s=%s is above memory_limit=%s, so the process cap could never be reached; lower %1$s or raise memory_limit.',
                self::ENV,
                $value,
                $limit,
            ));
        }

        return $cap;
    }
}
