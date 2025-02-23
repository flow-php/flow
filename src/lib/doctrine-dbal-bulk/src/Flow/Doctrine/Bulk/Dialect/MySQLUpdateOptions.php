<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\UpdateOptions;

final class MySQLUpdateOptions implements UpdateOptions
{
    public static function fromArray(array $options) : UpdateOptions
    {
        return new self();
    }

    public static function new() : UpdateOptions
    {
        return new self();
    }
}
