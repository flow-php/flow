<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security;

use SensitiveParameter;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;

/**
 * Contributes custom attributes derived from the authenticated token to the active request span.
 */
interface UserSpanAttributeProvider
{
    /**
     * @return array<string, scalar|array<scalar>>
     */
    public function attributes(#[SensitiveParameter] TokenInterface $token): array;
}
