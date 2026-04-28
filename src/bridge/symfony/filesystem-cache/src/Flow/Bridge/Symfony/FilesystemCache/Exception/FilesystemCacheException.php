<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemCache\Exception;

use Symfony\Component\Cache\Exception\InvalidArgumentException;

final class FilesystemCacheException extends InvalidArgumentException
{
    public static function corruptedCacheFile(string $path) : self
    {
        return new self(\sprintf('Cache file "%s" is corrupted (missing expiry/id/value sections).', $path));
    }

    public static function writeFailed(string $path, string $reason) : self
    {
        return new self(\sprintf('Failed to write cache file "%s": %s', $path, $reason));
    }
}
