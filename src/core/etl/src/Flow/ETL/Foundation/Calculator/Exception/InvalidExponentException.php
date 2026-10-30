<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Calculator\Exception;

final class InvalidExponentException extends Exception
{
    public function __construct(string $exponent)
    {
        parent::__construct(sprintf('Exponent "%s" is invalid. It must be an integer.', $exponent), 0, null);
    }
}
