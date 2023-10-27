<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

final class MissingDependencyException extends RuntimeException
{
    public function __construct(string $name, string $package, ?\Exception $previous = null)
    {
        parent::__construct(
            "Missing {$name} dependency, please run 'composer require {$package}'",
            previous: $previous
        );
    }
}
