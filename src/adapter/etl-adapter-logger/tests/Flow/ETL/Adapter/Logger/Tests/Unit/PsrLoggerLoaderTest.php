<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger\Tests\Unit;

use function Flow\ETL\DSL\{int_entry, string_entry};
use Flow\ETL\Adapter\Logger\PsrLoggerLoader;
use Flow\ETL\{Config, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;
use Psr\Log\Test\TestLogger;

final class PsrLoggerLoaderTest extends TestCase
{
    public function test_psr_logger_loader() : void
    {
        $logger = new TestLogger();

        $loader = new PsrLoggerLoader($logger, 'row log', LogLevel::ERROR);

        $loader->load(new Rows(
            Row::create(
                int_entry('id', 12345),
                string_entry('name', 'Norbert')->toLowercase()
            )
        ), new FlowContext(Config::default()));

        self::assertTrue($logger->hasErrorRecords());
        self::assertTrue($logger->hasError('row log'));
    }
}
