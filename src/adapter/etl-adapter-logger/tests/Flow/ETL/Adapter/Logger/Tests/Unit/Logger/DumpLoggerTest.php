<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger\Tests\Unit\Logger;

use Flow\ETL\Adapter\Logger\Logger\DumpLogger;
use PHPUnit\Framework\TestCase;

final class DumpLoggerTest extends TestCase
{
    public function test_logger() : void
    {
        if (\extension_loaded('xdebug')) {
            $this->markTestSkipped('Xdebug extension is loaded and it will affect DumpLogger');
        }

        $logger = new DumpLogger();

        \ob_start();
        $logger->error('error', ['id' => 1]);
        $output = \ob_get_contents();
        \ob_end_clean();

        $this->assertStringContainsString(
            <<<'OUTPUT'
array(1) {
  ["error"]=>
  array(1) {
    ["id"]=>
    int(1)
  }
}
OUTPUT,
            $output
        );
    }
}
