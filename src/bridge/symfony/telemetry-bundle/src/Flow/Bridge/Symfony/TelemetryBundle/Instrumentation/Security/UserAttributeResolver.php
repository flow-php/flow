<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security;

use SensitiveParameter;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;

use function is_scalar;
use function method_exists;

/**
 * Resolves span attributes from an authenticated token. A null attribute name disables that field.
 */
final readonly class UserAttributeResolver
{
    /**
     * @param iterable<UserSpanAttributeProvider> $providers
     */
    public function __construct(
        private iterable $providers,
        private ?string $idAttribute,
        private ?string $rolesAttribute,
        private ?string $emailAttribute,
        private string $emailGetter,
    ) {}

    /**
     * @return array<string, scalar|array<scalar>>
     */
    public function resolve(#[SensitiveParameter] TokenInterface $token): array
    {
        $attributes = [];

        if ($this->idAttribute !== null && ($identifier = $token->getUserIdentifier()) !== '') {
            $attributes[$this->idAttribute] = $identifier;
        }

        if ($this->rolesAttribute !== null && ($roles = $token->getRoleNames()) !== []) {
            $attributes[$this->rolesAttribute] = $roles;
        }

        if ($this->emailAttribute !== null) {
            $user = $token->getUser();

            if ($user !== null && method_exists($user, $this->emailGetter)) {
                // @mago-expect analysis:mixed-assignment
                // @mago-expect analysis:string-member-selector
                $value = $user->{$this->emailGetter}();

                if (is_scalar($value)) {
                    $attributes[$this->emailAttribute] = $value;
                }
            }
        }

        foreach ($this->providers as $provider) {
            foreach ($provider->attributes($token) as $key => $value) {
                $attributes[$key] = $value;
            }
        }

        return $attributes;
    }
}
