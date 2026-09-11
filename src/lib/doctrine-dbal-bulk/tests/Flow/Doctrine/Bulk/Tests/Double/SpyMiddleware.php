<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Double;

use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Middleware;

final class SpyMiddleware implements Middleware
{
    public int $prepares = 0;

    public function wrap(Driver $driver): Driver
    {
        return new SpyDriver($driver, $this);
    }
}
