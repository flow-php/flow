<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Adapter\Logger;

use Flow\ETL\Adapter\Logger\PsrLoggerLoader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
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
                new Row\Entry\IntegerEntry('id', 12345),
                Row\Entry\StringEntry::lowercase('name', 'Norbert')
            )
        ));

        $this->assertTrue($logger->hasErrorRecords());
        $this->assertTrue($logger->hasError('row log'));
    }
}
