<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\RouteNamePathMap;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Context\TempDirectoryContext;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Routing\FakeRouter;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Resource\FileResource;
use Symfony\Component\Routing\Route;
use Symfony\Component\Routing\RouteCollection;

use function chmod;
use function file_exists;
use function file_put_contents;
use function time;
use function touch;

#[CoversClass(RouteNamePathMap::class)]
final class RouteNamePathMapTest extends TestCase
{
    public function test_path_for_degrades_to_null_when_the_map_cannot_be_written(): void
    {
        TempDirectoryContext::with(static function (string $directory): void {
            $routes = new RouteCollection();
            $routes->add('order_show', new Route('/orders/{id}'));
            $router = new FakeRouter($routes);

            chmod($directory, 0o555);

            $map = new RouteNamePathMap($router, $directory, false);

            static::assertNull($map->pathFor('order_show'));
            static::assertNull($map->pathFor('order_show'));
            static::assertNull((new RouteNamePathMap($router, $directory, false))->pathFor('order_show'));
            static::assertSame(0, $router->getRouteCollectionCalls);
        });
    }

    public function test_path_for_does_not_touch_the_router_once_the_map_is_written(): void
    {
        TempDirectoryContext::with(static function (string $directory): void {
            $routes = new RouteCollection();
            $routes->add('order_show', new Route('/orders/{id}'));

            (new RouteNamePathMap(new FakeRouter($routes), $directory, false))->warmUp('');

            $router = new FakeRouter($routes);
            $map = new RouteNamePathMap($router, $directory, false);

            static::assertSame('/orders/{id}', $map->pathFor('order_show'));
            static::assertSame(0, $router->getRouteCollectionCalls);
        });
    }

    public function test_path_for_lazily_warms_the_map_when_never_warmed(): void
    {
        TempDirectoryContext::with(static function (string $directory): void {
            $routes = new RouteCollection();
            $routes->add('order_show', new Route('/orders/{id}'));

            $map = new RouteNamePathMap(new FakeRouter($routes), $directory, false);

            static::assertSame('/orders/{id}', $map->pathFor('order_show'));
        });
    }

    public function test_path_for_returns_null_for_unknown_route(): void
    {
        TempDirectoryContext::with(static function (string $directory): void {
            $routes = new RouteCollection();
            $routes->add('order_show', new Route('/orders/{id}'));

            $map = new RouteNamePathMap(new FakeRouter($routes), $directory, false);

            static::assertNull($map->pathFor('unknown_route'));
        });
    }

    public function test_path_for_returns_null_without_router(): void
    {
        TempDirectoryContext::with(static function (string $directory): void {
            $map = new RouteNamePathMap(null, $directory, false);

            static::assertNull($map->pathFor('order_show'));
        });
    }

    public function test_stale_map_is_rebuilt_in_debug_when_route_resources_change(): void
    {
        TempDirectoryContext::with(static function (string $directory): void {
            $resourceFile = $directory . '/routes.yaml';
            file_put_contents($resourceFile, 'order_show: /orders/{id}');

            $routes = new RouteCollection();
            $routes->add('order_show', new Route('/orders/{id}'));
            $routes->addResource(new FileResource($resourceFile));

            $router = new FakeRouter($routes);

            (new RouteNamePathMap($router, $directory, true))->warmUp('');

            $changedRoutes = new RouteCollection();
            $changedRoutes->add('order_show', new Route('/purchases/{id}'));
            $changedRoutes->addResource(new FileResource($resourceFile));
            $router->setRouteCollection($changedRoutes);
            touch($resourceFile, time() + 10);

            static::assertSame(
                '/purchases/{id}',
                (new RouteNamePathMap($router, $directory, true))->pathFor('order_show'),
            );
        });
    }

    public function test_warm_up_returns_the_map_file_for_preloading(): void
    {
        TempDirectoryContext::with(static function (string $directory): void {
            $routes = new RouteCollection();
            $routes->add('order_show', new Route('/orders/{id}'));

            $preload = (new RouteNamePathMap(new FakeRouter($routes), $directory, false))->warmUp('');

            static::assertSame([$directory . '/flow_telemetry_route_paths.php'], $preload);
            static::assertFileExists($directory . '/flow_telemetry_route_paths.php');
        });
    }

    public function test_warm_up_without_router_writes_nothing(): void
    {
        TempDirectoryContext::with(static function (string $directory): void {
            $map = new RouteNamePathMap(null, $directory, false);

            static::assertSame([], $map->warmUp(''));
            static::assertFalse(file_exists($directory . '/flow_telemetry_route_paths.php'));
        });
    }

    public function test_warmer_is_optional(): void
    {
        TempDirectoryContext::with(static function (string $directory): void {
            static::assertTrue((new RouteNamePathMap(null, $directory, false))->isOptional());
        });
    }
}
