<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\ErrorHandler;

use Flow\Telemetry\ErrorHandler\SyslogHandler;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class SyslogHandlerTest extends TestCase
{
    public function test_constructs_with_defaults(): void
    {
        $handler = new SyslogHandler();

        static::assertInstanceOf(SyslogHandler::class, $handler);
    }

    public function test_throws_on_empty_ident(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new SyslogHandler(ident: '');
    }
}
