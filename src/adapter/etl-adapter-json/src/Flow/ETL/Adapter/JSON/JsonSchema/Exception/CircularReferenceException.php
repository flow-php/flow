<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JsonSchema\Exception;

use Flow\ETL\Exception\RuntimeException;

use function implode;
use function sprintf;

final class CircularReferenceException extends RuntimeException
{
    /**
     * @param array<string> $chain
     */
    public function __construct(string $ref, array $chain)
    {
        parent::__construct(sprintf(
            'Circular reference detected: "%s", resolution chain: %s',
            $ref,
            implode(' -> ', $chain),
        ));
    }
}
