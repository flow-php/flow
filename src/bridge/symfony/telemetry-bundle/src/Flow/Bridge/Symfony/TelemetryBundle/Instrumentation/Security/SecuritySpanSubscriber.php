<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Telemetry\Tracer\Span;
use SensitiveParameter;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Event\ControllerEvent;
use Symfony\Component\HttpKernel\KernelEvents;
use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorageInterface;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;
use Symfony\Component\Security\Http\Event\LoginSuccessEvent;

use function class_exists;

final readonly class SecuritySpanSubscriber implements EventSubscriberInterface
{
    public function __construct(
        private TokenStorageInterface $tokenStorage,
        private UserAttributeResolver $resolver,
    ) {}

    public static function getSubscribedEvents(): array
    {
        $events = [
            KernelEvents::CONTROLLER => ['onController', 0],
        ];

        if (class_exists(LoginSuccessEvent::class)) {
            $events[LoginSuccessEvent::class] = ['onLoginSuccess', 0];
        }

        return $events;
    }

    public function onController(ControllerEvent $event): void
    {
        $token = $this->tokenStorage->getToken();

        if ($token === null) {
            return;
        }

        $this->decorate($event->getRequest(), $token);
    }

    public function onLoginSuccess(LoginSuccessEvent $event): void
    {
        $this->decorate($event->getRequest(), $event->getAuthenticatedToken());
    }

    private function decorate(Request $request, #[SensitiveParameter] TokenInterface $token): void
    {
        // @mago-expect analysis:mixed-assignment
        if (!($span = $request->attributes->get(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE)) instanceof Span) {
            return;
        }

        foreach ($this->resolver->resolve($token) as $key => $value) {
            $span->setAttribute($key, $value);
        }
    }
}
