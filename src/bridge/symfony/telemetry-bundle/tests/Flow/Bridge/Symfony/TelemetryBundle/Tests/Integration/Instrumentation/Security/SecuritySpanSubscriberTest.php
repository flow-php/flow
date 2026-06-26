<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Security;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security\SecuritySpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security\UserAttributeResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller\TestController;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security\FakeAuthenticator;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security\StaticUserAttributeProvider;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security\TestSecurityUser;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Routing\Route;
use Symfony\Component\Routing\Router;
use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorage;
use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorageInterface;
use Symfony\Component\Security\Core\Authentication\Token\UsernamePasswordToken;
use Symfony\Component\Security\Http\Authenticator\Passport\Badge\UserBadge;
use Symfony\Component\Security\Http\Authenticator\Passport\SelfValidatingPassport;
use Symfony\Component\Security\Http\Event\LoginSuccessEvent;

#[CoversClass(SecuritySpanSubscriber::class)]
#[CoversClass(UserAttributeResolver::class)]
final class SecuritySpanSubscriberTest extends KernelTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_decorates_request_span_with_authenticated_user_and_provider_attributes(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../../Fixtures/config/routes.php',
                    ],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => true],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                        'security' => [
                            'enabled' => true,
                            'fields' => [
                                'roles' => ['enabled' => true],
                                'email' => ['enabled' => true],
                            ],
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->setDefinition('security.token_storage', new Definition(TokenStorage::class))
                        ->setPublic(true);
                    $container
                        ->setDefinition('test.user_attributes', new Definition(StaticUserAttributeProvider::class))
                        ->setArgument(0, ['app.tenant_id' => 'acme'])
                        ->addTag('flow.telemetry.security.user_attribute_provider');
                });
            },
        ]);

        $container = $this->getContainer();

        /** @var Router $router */
        $router = $container->get('router');
        $router->getRouteCollection()->add('test_index', new Route('/test', [
            '_controller' => TestController::class . '::index',
        ]));

        /** @var TokenStorageInterface $tokenStorage */
        $tokenStorage = $container->get('security.token_storage');
        $user = new TestSecurityUser('alice', 'alice@example.com', ['ROLE_USER', 'ROLE_ADMIN']);
        $tokenStorage->setToken(new UsernamePasswordToken($user, 'main', $user->getRoles()));

        $request = Request::create('/test', 'GET');
        $kernel->terminate($request, $kernel->handle($request));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $serverSpan = array_values(array_filter(
            $processor->endedSpans(),
            static fn(Span $s): bool => $s->kind() === SpanKind::SERVER,
        ))[0];

        $attributes = $serverSpan->attributes();
        static::assertSame('alice', $attributes['user.id']);
        static::assertSame(['ROLE_USER', 'ROLE_ADMIN'], $attributes['user.roles']);
        static::assertSame('alice@example.com', $attributes['user.email']);
        static::assertSame('acme', $attributes['app.tenant_id']);
    }

    public function test_decorates_request_span_on_login_success(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                        'security' => ['enabled' => true],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->setDefinition('security.token_storage', new Definition(TokenStorage::class))
                        ->setPublic(true);
                });
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $subscriber = $this->symfonyContext()->getService(
            'flow.telemetry.security.span_subscriber',
            SecuritySpanSubscriber::class,
        );

        $span = $telemetry->tracer('test')->span('GET /test', SpanKind::SERVER);

        $request = Request::create('/test', 'GET');
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $span);

        $user = new TestSecurityUser('bob');

        $subscriber->onLoginSuccess(
            new LoginSuccessEvent(
                new FakeAuthenticator(),
                new SelfValidatingPassport(new UserBadge('bob', static fn(): TestSecurityUser => $user)),
                new UsernamePasswordToken($user, 'main'),
                $request,
                null,
                'main',
            ),
        );

        static::assertSame('bob', $span->attributes()['user.id']);
    }
}
