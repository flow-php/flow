<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Foundation\Exception;

use Flow\ETL\Foundation\Exception\RuntimeException;

use function sprintf;

final class UnresolvableReferenceException extends RuntimeException
{
    public function __construct(string $ref, string $reason)
    {
        parent::__construct(sprintf('Cannot resolve reference "%s": %s', $ref, $reason));
    }
}
