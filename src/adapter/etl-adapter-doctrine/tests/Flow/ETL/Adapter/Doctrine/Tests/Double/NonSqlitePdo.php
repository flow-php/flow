<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Double;

use PDO;

final class NonSqlitePdo extends PDO
{
    public function __construct()
    {
        parent::__construct('sqlite::memory:');
    }

    public function getAttribute(int $attribute): mixed
    {
        return $attribute === PDO::ATTR_DRIVER_NAME ? 'pgsql' : parent::getAttribute($attribute);
    }
}
