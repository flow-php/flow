<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Runtime\CacheConfig;
use Flow\Filesystem\Path;
use RuntimeException;

use function getcwd;
use function sprintf;

final class CacheDirContext
{
    /**
     * @throws RuntimeException
     */
    public static function assertSafe(Path $cacheDir): void
    {
        $resolved = $cacheDir->path();

        if ($resolved === '' || $resolved === '/' || $resolved === '\\') {
            throw new RuntimeException(sprintf(
                'Refusing to run integration tests with cache dir resolved to "%s" — would delete root.',
                $resolved,
            ));
        }

        if ($resolved === getcwd()) {
            throw new RuntimeException(sprintf(
                'Refusing to run integration tests with cache dir equal to the current working directory ("%s"). '
                . 'Set %s to a dedicated temp directory.',
                $resolved,
                CacheConfig::CACHE_DIR_ENV,
            ));
        }
    }
}
