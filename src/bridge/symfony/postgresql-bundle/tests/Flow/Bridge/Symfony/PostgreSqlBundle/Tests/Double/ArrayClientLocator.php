<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Double;

use Psr\Container\{ContainerInterface, NotFoundExceptionInterface};

final class ArrayClientLocator implements ContainerInterface
{
    /**
     * @param array<string, mixed> $services
     */
    public function __construct(
        public array $services = [],
    ) {
    }

    public function get(string $id) : mixed
    {
        if (!\array_key_exists($id, $this->services)) {
            throw new class(\sprintf('Service "%s" not found.', $id)) extends \RuntimeException implements NotFoundExceptionInterface {};
        }

        return $this->services[$id];
    }

    public function has(string $id) : bool
    {
        return \array_key_exists($id, $this->services);
    }
}
