<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration;

use Monolog\Handler\StreamHandler;
use Monolog\Level;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class ParquetIntegrationTestCase extends TestCase
{
    protected function getLogger() : LoggerInterface
    {
        if ((int) \getenv('FLOW_PARQUET_TESTS_DEBUG')) {
            $logger = new Logger('test');
            $logger->pushHandler(new StreamHandler('php://stdout', Level::Info));
        } else {
            $logger = new NullLogger();
        }

        return $logger;
    }
}
