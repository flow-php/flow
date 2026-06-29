<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security;

use Symfony\Component\Security\Core\User\UserInterface;

final class TestSecurityUser implements UserInterface
{
    /**
     * @param non-empty-string $identifier
     * @param array<string> $roles
     */
    public function __construct(
        private readonly string $identifier,
        private readonly ?string $email = null,
        private readonly array $roles = [],
    ) {}

    public function eraseCredentials(): void {}

    public function getEmail(): ?string
    {
        return $this->email;
    }

    /**
     * @return array<string, null|string>
     */
    public function getProfile(): array
    {
        return ['email' => $this->email];
    }

    public function getRoles(): array
    {
        return $this->roles;
    }

    /**
     * @return non-empty-string
     */
    public function getUserIdentifier(): string
    {
        return $this->identifier;
    }
}
