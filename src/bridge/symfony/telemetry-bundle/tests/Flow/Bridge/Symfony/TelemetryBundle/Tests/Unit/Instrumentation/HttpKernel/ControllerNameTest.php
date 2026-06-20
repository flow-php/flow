<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\ControllerName;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller\TestController;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(ControllerName::class)]
final class ControllerNameTest extends TestCase
{
    public function test_resolves_class_method_array(): void
    {
        $resolved = ControllerName::resolve([TestController::class, 'index']);

        static::assertNotNull($resolved);
        static::assertSame(TestController::class . '::index', $resolved->name);
        static::assertSame(TestController::class, $resolved->namespace);
        static::assertSame('index', $resolved->function);
    }

    public function test_resolves_object_method_array(): void
    {
        $resolved = ControllerName::resolve([new TestController(), 'index']);

        static::assertNotNull($resolved);
        static::assertSame(TestController::class . '::index', $resolved->name);
        static::assertSame(TestController::class, $resolved->namespace);
        static::assertSame('index', $resolved->function);
    }

    public function test_resolves_invokable_object(): void
    {
        $controller = new class {
            public function __invoke(): void {}
        };

        $resolved = ControllerName::resolve($controller);

        static::assertNotNull($resolved);
        static::assertSame($controller::class . '::__invoke', $resolved->name);
        static::assertSame($controller::class, $resolved->namespace);
        static::assertSame('__invoke', $resolved->function);
    }

    public function test_resolves_closure(): void
    {
        $resolved = ControllerName::resolve(static function (): void {});

        static::assertNotNull($resolved);
        static::assertSame('Closure', $resolved->name);
        static::assertNull($resolved->namespace);
        static::assertNull($resolved->function);
    }

    public function test_resolves_string_controller(): void
    {
        $resolved = ControllerName::resolve('strtoupper');

        static::assertNotNull($resolved);
        static::assertSame('strtoupper', $resolved->name);
        static::assertNull($resolved->namespace);
        static::assertNull($resolved->function);
    }

    public function test_returns_null_for_unsupported_array_shape(): void
    {
        static::assertNull(ControllerName::resolve([TestController::class, 'index', 'extra']));
    }
}
