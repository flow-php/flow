<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Security;

use SensitiveParameter;
use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorageInterface;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;

final class SpyTokenStorage implements TokenStorageInterface
{
    private int $reads = 0;

    public function __construct(
        #[SensitiveParameter]
        private ?TokenInterface $token = null,
    ) {}

    public function getToken(): ?TokenInterface
    {
        $this->reads++;

        return $this->token;
    }

    public function reads(): int
    {
        return $this->reads;
    }

    public function setToken(#[SensitiveParameter] ?TokenInterface $token): void
    {
        $this->token = $token;
    }
}
