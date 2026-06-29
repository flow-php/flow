<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security;

use RuntimeException;
use SensitiveParameter;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;
use Symfony\Component\Security\Core\Exception\AuthenticationException;
use Symfony\Component\Security\Http\Authenticator\AuthenticatorInterface;
use Symfony\Component\Security\Http\Authenticator\Passport\Passport;

/**
 * No-op authenticator used only to construct a LoginSuccessEvent in tests; never executed.
 */
final class FakeAuthenticator implements AuthenticatorInterface
{
    public function authenticate(Request $request): Passport
    {
        throw new RuntimeException('not used');
    }

    public function createToken(Passport $passport, string $firewallName): TokenInterface
    {
        throw new RuntimeException('not used');
    }

    public function onAuthenticationFailure(Request $request, AuthenticationException $exception): ?Response
    {
        return null;
    }

    public function onAuthenticationSuccess(
        Request $request,
        #[SensitiveParameter]
        TokenInterface $token,
        string $firewallName,
    ): ?Response {
        return null;
    }

    public function supports(Request $request): ?bool
    {
        return false;
    }
}
