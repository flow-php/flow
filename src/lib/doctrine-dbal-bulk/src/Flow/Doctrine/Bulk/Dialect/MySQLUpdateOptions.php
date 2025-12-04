<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\UpdateOptions;

final class MySQLUpdateOptions implements UpdateOptions
{
    #[\Override]
    public static function fromArray(array $options) : UpdateOptions
    {
        return new self();
    }

    #[\Override]
    public static function new() : UpdateOptions
    {
        return new self();
    }
}
