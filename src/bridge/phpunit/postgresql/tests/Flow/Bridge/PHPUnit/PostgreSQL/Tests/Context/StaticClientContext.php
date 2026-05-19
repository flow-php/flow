<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Context;

use Flow\Bridge\PHPUnit\PostgreSQL\StaticClient;
use Flow\PostgreSql\Client\Client;
use ReflectionClass;

final class StaticClientContext
{
    public static function injectClient(string $key, Client $client): void
    {
        self::injectClients([$key => $client]);
    }

    /**
     * @param array<string, Client> $clients
     */
    public static function injectClients(array $clients): void
    {
        $reflection = new ReflectionClass(StaticClient::class);
        $property = $reflection->getProperty('clients');
        $property->setValue(null, $clients);
    }
}
