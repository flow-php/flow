<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Memory;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function getenv;
use function ini_get;
use function is_string;
use function sprintf;
use function str_starts_with;

final readonly class MemoryLimit
{
    public const int PERCENTAGE_OF_MEMORY_LIMIT = 70;

    public const string ENV = 'FLOW_MAX_MEMORY';

    public const int WITHOUT_MEMORY_LIMIT_GB = 1;

    /**
     * @throws InvalidArgumentException
     */
    public static function default(): Unit
    {
        $cap = self::processCap();

        if ($cap !== null) {
            return $cap;
        }

        $value = getenv(self::ENV);

        return is_string($value) && $value !== ''
            ? Unit::fromString($value)
            : Unit::fromGb(self::WITHOUT_MEMORY_LIMIT_GB);
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
