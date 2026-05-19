<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\ErrorHandler;

use Flow\Telemetry\ErrorHandler\CompositeErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

final class CompositeErrorHandlerTest extends TestCase
{
    public function test_calls_each_child_handler_in_order(): void
    {
        $first = new ErrorHandlerSpy();
        $second = new ErrorHandlerSpy();
        $error = new RuntimeException('boom');

        $composite = new CompositeErrorHandler($first, $second);
        $composite->handle($error);

        static::assertSame($error, $first->last());
        static::assertSame($error, $second->last());
    }

    public function test_continues_when_a_child_throws(): void
    {
        $throwing = new class implements ErrorHandler {
            public function handle(Throwable $error): void
            {
                throw new RuntimeException('child blew up');
            }
        };
        $sibling = new ErrorHandlerSpy();

        $composite = new CompositeErrorHandler($throwing, $sibling);
        $composite->handle(new RuntimeException('boom'));

        static::assertSame(1, $sibling->count());
    }

    public function test_exposes_handlers(): void
    {
        $first = new ErrorHandlerSpy();
        $second = new ErrorHandlerSpy();

        $composite = new CompositeErrorHandler($first, $second);

        static::assertSame([$first, $second], $composite->handlers());
    }

    public function test_handles_an_empty_handler_list(): void
    {
        $this->expectNotToPerformAssertions();

        (new CompositeErrorHandler())->handle(new RuntimeException('boom'));
    }
}
