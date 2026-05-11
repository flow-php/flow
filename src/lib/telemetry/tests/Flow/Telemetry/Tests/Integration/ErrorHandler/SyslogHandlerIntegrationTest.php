<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\ErrorHandler;

use Flow\Telemetry\ErrorHandler\SyslogHandler;
use PHPUnit\Framework\TestCase;

final class SyslogHandlerIntegrationTest extends TestCase
{
    public function test_does_not_throw_when_writing_to_syslog(): void
    {
        $this->expectNotToPerformAssertions();

        $handler = new SyslogHandler(ident: 'flow-telemetry-test');

        $handler->handle(new \RuntimeException('integration test'));
    }
}
