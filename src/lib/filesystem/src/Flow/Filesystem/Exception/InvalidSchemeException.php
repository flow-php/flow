<?php

declare(strict_types=1);

namespace Flow\Filesystem\Exception;

final class InvalidSchemeException extends Exception
{
    public function __construct(string $scheme, string $expectedScheme)
    {
        parent::__construct(\sprintf('Scheme "%s" is not supported by this protocol. Expected scheme is "%s"', $scheme, $expectedScheme));
    }
}
