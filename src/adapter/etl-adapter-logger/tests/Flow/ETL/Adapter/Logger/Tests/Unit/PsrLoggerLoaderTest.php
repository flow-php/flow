<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger\Tests\Unit;

use Flow\ETL\Adapter\Logger\PsrLoggerLoader;
use Flow\ETL\Tests\FlowTestCase;
use Psr\Log\LogLevel;
use Psr\Log\Test\TestLogger;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;

final class PsrLoggerLoaderTest extends FlowTestCase
{
    public function test_psr_logger_loader(): void
    {
        $logger = new TestLogger();

        $loader = new PsrLoggerLoader($logger, 'row log', LogLevel::ERROR);

        $loader->load(rows(row(int_entry('id', 12345), string_entry('name', 'Norbert'))), flow_context(config()));

        static::assertTrue($logger->hasErrorRecords());
        static::assertTrue($logger->hasError('row log'));
    }
}
