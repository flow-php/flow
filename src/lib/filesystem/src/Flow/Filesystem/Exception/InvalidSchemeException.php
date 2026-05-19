<?php

declare(strict_types=1);

namespace Flow\Filesystem\Exception;

use function sprintf;

final class InvalidSchemeException extends Exception
{
    public function __construct(string $protocol, string $expectedProtocol)
    {
        parent::__construct(sprintf(
            'Scheme "%s://" is not supported by this protocol. Expected scheme is "%s://"',
            $protocol,
            $expectedProtocol,
        ));
    }
}
