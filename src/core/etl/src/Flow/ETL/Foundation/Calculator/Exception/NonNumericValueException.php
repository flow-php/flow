<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Calculator\Exception;

final class NonNumericValueException extends Exception
{
    public function __construct(string $value)
    {
        parent::__construct(sprintf('Value "%s" is not numeric.', $value), 0, null);
    }
}
