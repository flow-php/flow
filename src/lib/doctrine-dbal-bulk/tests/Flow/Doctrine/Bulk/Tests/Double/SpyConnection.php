<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Double;

use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Middleware\AbstractConnectionMiddleware;
use Doctrine\DBAL\Driver\Statement;

final class SpyConnection extends AbstractConnectionMiddleware
{
    public function __construct(
        Connection $connection,
        private readonly SpyMiddleware $spy,
    ) {
        parent::__construct($connection);
    }

    public function prepare(string $sql): Statement
    {
        $this->spy->prepares++;

        return parent::prepare($sql);
    }
}
