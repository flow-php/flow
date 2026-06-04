<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures;

final readonly class MigrationSeedProvider
{
    public function __construct(
        private string $value = 'seeded-by-service',
    ) {}

    public function value(): string
    {
        return $this->value;
    }
}
