<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function array_map;
use function implode;
use function sprintf;

enum MultisetHashAlgorithm: string
{
    case md5 = 'md5';
    case sha256 = 'sha256';
    case xxh128 = 'xxh128';

    /**
     * @throws InvalidArgumentException
     */
    public static function named(string $name): self
    {
        return (
            self::tryFrom($name) ?? throw new InvalidArgumentException(sprintf(
                'Unknown multiset hash algorithm "%s", use one of: %s',
                $name,
                implode(', ', array_map(static fn(self $algorithm): string => $algorithm->value, self::cases())),
            ))
        );
    }
}
