<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security\UserSpanAttributeProvider;
use SensitiveParameter;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;

final class StaticUserAttributeProvider implements UserSpanAttributeProvider
{
    /**
     * @param array<string, scalar|array<scalar>> $attributes
     */
    public function __construct(
        private readonly array $attributes,
    ) {}

    public function attributes(#[SensitiveParameter] TokenInterface $token): array
    {
        return $this->attributes;
    }
}
