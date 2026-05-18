<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Throwable;

use function sprintf;

final class MissingExtensionException extends Exception
{
    public function __construct(string $extension = '', int $code = 0, ?Throwable $previous = null)
    {
        parent::__construct(
            sprintf('Missing extension %s, please check available extensions using CLI command `php -m` ', $extension),
            $code,
            $previous,
        );
    }
}
