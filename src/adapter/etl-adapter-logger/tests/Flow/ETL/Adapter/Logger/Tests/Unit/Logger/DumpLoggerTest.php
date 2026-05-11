<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger\Tests\Unit\Logger;

use Flow\ETL\Adapter\Logger\Logger\DumpLogger;
use Flow\ETL\Tests\FlowTestCase;

final class DumpLoggerTest extends FlowTestCase
{
    public function test_logger(): void
    {
        if (\extension_loaded('xdebug')) {
            static::markTestSkipped('Xdebug extension is loaded and it will affect DumpLogger');
        }

        if (\class_exists('\\Symfony\\Component\\VarDumper\\VarDumper')) {
            static::markTestSkipped('Symfony VarDumper is loaded and it will affect DumpLogger output format');
        }

        $logger = new DumpLogger();

        \ob_start();
        $logger->error('error', ['id' => 1]);
        $output = \ob_get_contents();
        \ob_end_clean();

        if ($output === false) {
            static::fail('Failed to get output buffer contents');
        }

        static::assertStringContainsString(<<<'OUTPUT'
            array(1) {
              ["error"]=>
              array(1) {
                ["id"]=>
                int(1)
              }
            }
            OUTPUT, $output);
    }
}
