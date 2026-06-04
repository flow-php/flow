<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Migrations\Exception\MigrationException;

use function array_key_exists;

final readonly class MigrationContext
{
    /**
     * @param array<string, mixed> $attributes
     */
    public function __construct(
        public Client $client,
        public array $attributes = [],
    ) {}

    public function attribute(string $name): mixed
    {
        if (!array_key_exists($name, $this->attributes)) {
            throw MigrationException::attributeNotFound($name);
        }

        return $this->attributes[$name];
    }

    public function hasAttribute(string $name): bool
    {
        return array_key_exists($name, $this->attributes);
    }
}
