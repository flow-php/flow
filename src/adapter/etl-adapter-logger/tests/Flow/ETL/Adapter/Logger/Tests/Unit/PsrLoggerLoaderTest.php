<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger\Tests\Unit;

use function Flow\ETL\DSL\{config, flow_context, row, rows};
use function Flow\ETL\DSL\{int_entry, string_entry};
use Flow\ETL\Adapter\Logger\PsrLoggerLoader;
use Flow\ETL\{Tests\FlowTestCase};
use Psr\Log\LogLevel;
use Psr\Log\Test\TestLogger;

final class PsrLoggerLoaderTest extends FlowTestCase
{
    public function test_psr_logger_loader() : void
    {
        $logger = new TestLogger();

        $loader = new PsrLoggerLoader($logger, 'row log', LogLevel::ERROR);

        $loader->load(rows(row(int_entry('id', 12345), string_entry('name', 'Norbert')->toLowercase())), flow_context(config()));

        self::assertTrue($logger->hasErrorRecords());
        self::assertTrue($logger->hasError('row log'));
    }
}
