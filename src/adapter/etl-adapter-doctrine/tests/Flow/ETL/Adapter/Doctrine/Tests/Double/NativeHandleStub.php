<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Double;

use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Middleware;
use Doctrine\DBAL\Driver\Middleware\AbstractConnectionMiddleware;
use Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware;

/**
 * Makes Connection::getNativeConnection() answer with an arbitrary object, so the dispatch's
 * default arm is reachable without a driver flow has no arm for.
 */
final readonly class NativeHandleStub implements Middleware
{
    public function __construct(
        private object $native,
    ) {}

    public function wrap(Driver $driver): Driver
    {
        return new class($driver, $this->native) extends AbstractDriverMiddleware {
            public function __construct(
                Driver $driver,
                private readonly object $native,
            ) {
                parent::__construct($driver);
            }

            public function connect(array $params): Connection
            {
                return new class(parent::connect($params), $this->native) extends AbstractConnectionMiddleware {
                    public function __construct(
                        Connection $connection,
                        private readonly object $native,
                    ) {
                        parent::__construct($connection);
                    }

                    public function getNativeConnection(): object
                    {
                        return $this->native;
                    }
                };
            }
        };
    }
}
