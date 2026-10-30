<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Exception;

use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;

use function sprintf;

class InvalidArgumentException extends Exception
{
    public static function because(string $format, float|int|string ...$parameters): self
    {
        return new self(sprintf($format, ...$parameters));
    }

    /**
     * @param non-empty-string $dsl the function or method the caller passes the filesystem to
     */
    public static function unsupportedPath(Filesystem $filesystem, Path $path, string $dsl): self
    {
        return new self(sprintf(
            'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles this scheme, e.g. '
            . '%s($path, filesystem: aws_s3_filesystem(...)).',
            $filesystem::class,
            $filesystem->mount()->protocol,
            $path->uri(),
            $dsl,
        ));
    }
}
