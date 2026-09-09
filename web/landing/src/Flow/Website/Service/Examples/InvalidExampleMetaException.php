<?php

declare(strict_types=1);

namespace Flow\Website\Service\Examples;

use RuntimeException;

use function sprintf;

final class InvalidExampleMetaException extends RuntimeException
{
    public static function badValue(string $file, string $key, string $expected): self
    {
        return new self(sprintf('%s: "%s" must be %s.', $file, $key, $expected));
    }

    public static function unknownKey(string $file, string $key, string $known): self
    {
        return new self(sprintf('%s: unknown key "%s". Known keys: %s.', $file, $key, $known));
    }
}
