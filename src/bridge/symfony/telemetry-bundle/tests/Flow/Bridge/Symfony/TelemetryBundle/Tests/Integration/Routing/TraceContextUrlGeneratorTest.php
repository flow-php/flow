<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Routing;

use Flow\Bridge\Symfony\TelemetryBundle\Routing\TraceContextUrlGenerator;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller\TestController;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\Routing\Route;
use Symfony\Component\Routing\Router;

#[CoversClass(TraceContextUrlGenerator::class)]
final class TraceContextUrlGeneratorTest extends KernelTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_is_wired_with_the_real_router(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => ['utf8' => true, 'resource' => __DIR__ . '/../../Fixtures/config/routes.php'],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['void' => ['void' => null]],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var Router $router */
        $router = $container->get('router');
        $router->getRouteCollection()->add('test_index', new Route('/test', [
            '_controller' => TestController::class . '::index',
        ]));

        $generator = $this->symfonyContext()->getService(
            TraceContextUrlGenerator::class,
            TraceContextUrlGenerator::class,
        );

        // No active span outside a request, so the URL is returned untouched — this proves the router wiring.
        static::assertSame('/test', $generator->generate('test_index'));
    }
}
