<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Security;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security\SecuritySpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security\UserAttributeResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security\FakeAuthenticator;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security\SpyTokenStorage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security\TestSecurityUser;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Event\ControllerEvent;
use Symfony\Component\HttpKernel\HttpKernelInterface;
use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorage;
use Symfony\Component\Security\Core\Authentication\Token\UsernamePasswordToken;
use Symfony\Component\Security\Http\Authenticator\Passport\Badge\UserBadge;
use Symfony\Component\Security\Http\Authenticator\Passport\SelfValidatingPassport;
use Symfony\Component\Security\Http\Event\LoginSuccessEvent;

#[CoversClass(SecuritySpanSubscriber::class)]
final class SecuritySpanSubscriberTest extends TestCase
{
    public function test_does_not_decorate_when_there_is_no_token(): void
    {
        $telemetry = TelemetryMother::withSpanProcessor(new MemorySpanProcessor(new MemoryExporter()));
        $span = $telemetry->tracer('test')->span('GET /test', SpanKind::SERVER);

        $request = Request::create('/test', 'GET');
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $span);

        $subscriber = new SecuritySpanSubscriber(
            new TokenStorage(),
            new UserAttributeResolver([], 'user.id', null, null, 'getEmail'),
        );

        $subscriber->onController(
            new ControllerEvent(
                $this->createStub(HttpKernelInterface::class),
                static fn(): null => null,
                $request,
                HttpKernelInterface::MAIN_REQUEST,
            ),
        );

        static::assertArrayNotHasKey('user.id', $span->attributes());
    }

    public function test_does_not_decorate_on_login_success_when_request_has_no_span(): void
    {
        $user = new TestSecurityUser('bob');
        $token = new UsernamePasswordToken($user, 'main');

        $request = Request::create('/test', 'GET');

        $subscriber = new SecuritySpanSubscriber(
            new TokenStorage(),
            new UserAttributeResolver([], 'user.id', null, null, 'getEmail'),
        );

        $subscriber->onLoginSuccess(
            new LoginSuccessEvent(
                new FakeAuthenticator(),
                new SelfValidatingPassport(new UserBadge('bob', static fn(): TestSecurityUser => $user)),
                $token,
                $request,
                null,
                'main',
            ),
        );

        static::assertFalse($request->attributes->has(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE));
    }

    public function test_does_not_read_the_token_when_request_has_no_span(): void
    {
        $tokenStorage = new SpyTokenStorage(new UsernamePasswordToken(new TestSecurityUser('alice'), 'main'));

        $request = Request::create('/test', 'GET');
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, 'not-a-span');

        $subscriber = new SecuritySpanSubscriber(
            $tokenStorage,
            new UserAttributeResolver([], 'user.id', null, null, 'getEmail'),
        );

        $subscriber->onController(
            new ControllerEvent(
                $this->createStub(HttpKernelInterface::class),
                static fn(): null => null,
                $request,
                HttpKernelInterface::MAIN_REQUEST,
            ),
        );

        static::assertSame(0, $tokenStorage->reads());
        static::assertSame('not-a-span', $request->attributes->get(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE));
    }
}
