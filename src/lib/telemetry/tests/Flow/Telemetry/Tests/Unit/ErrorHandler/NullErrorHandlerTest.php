<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\ErrorHandler;

use Flow\Telemetry\ErrorHandler\NullErrorHandler;
use PHPUnit\Framework\TestCase;

final class NullErrorHandlerTest extends TestCase
{
    public function test_does_nothing_when_invoked() : void
    {
        $this->expectNotToPerformAssertions();

        $handler = new NullErrorHandler();

        $handler->handle(new \RuntimeException('boom'));
    }
}
