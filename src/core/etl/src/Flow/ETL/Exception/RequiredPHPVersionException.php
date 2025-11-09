<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

final class RequiredPHPVersionException extends RuntimeException
{
    public function __construct(string $className, string $version, ?\Exception $previous = null)
    {
        parent::__construct(
            "To use {$className} class, you need to upgrade your PHP version to: {$version}+.",
            previous: $previous
        );
    }
}
