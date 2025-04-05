<?php

declare(strict_types=1);

namespace Flow\Calculator\Exception;

final class InvalidScaleException extends Exception
{
    public function __construct(int $scale)
    {
        parent::__construct(sprintf('Scale "%d" is invalid. It must be between 0 and 16.', $scale), 0, null);
    }
}
