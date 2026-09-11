<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Double;

use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware;
use SensitiveParameter;

final class SpyDriver extends AbstractDriverMiddleware
{
    public function __construct(
        Driver $driver,
        private readonly SpyMiddleware $spy,
    ) {
        parent::__construct($driver);
    }

    public function connect(#[SensitiveParameter] array $params): Connection
    {
        return new SpyConnection(parent::connect($params), $this->spy);
    }
}
