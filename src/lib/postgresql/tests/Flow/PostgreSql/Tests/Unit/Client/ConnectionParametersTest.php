<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client;

use Flow\PostgreSql\Client\ConnectionParameters;
use PHPUnit\Framework\TestCase;

final class ConnectionParametersTest extends TestCase
{
    public function test_from_params_basic() : void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
        );

        self::assertStringContainsString('host=localhost', $params->connectionString);
        self::assertStringContainsString('port=5432', $params->connectionString);
        self::assertStringContainsString('dbname=testdb', $params->connectionString);
    }

    public function test_from_params_with_credentials() : void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
            host: 'db.example.com',
            port: 5433,
            user: 'admin',
            password: 'secret123',
        );

        self::assertStringContainsString('host=db.example.com', $params->connectionString);
        self::assertStringContainsString('port=5433', $params->connectionString);
        self::assertStringContainsString('dbname=testdb', $params->connectionString);
        self::assertStringContainsString('user=admin', $params->connectionString);
        self::assertStringContainsString('password=secret123', $params->connectionString);
    }

    public function test_from_params_with_options() : void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
            options: [
                'sslmode' => 'require',
                'connect_timeout' => '10',
            ],
        );

        self::assertStringContainsString('sslmode=require', $params->connectionString);
        self::assertStringContainsString('connect_timeout=10', $params->connectionString);
    }

    public function test_from_params_without_optional_credentials() : void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
        );

        self::assertStringNotContainsString('user=', $params->connectionString);
        self::assertStringNotContainsString('password=', $params->connectionString);
    }

    public function test_from_string() : void
    {
        $connectionString = 'host=localhost port=5432 dbname=test user=postgres password=secret';

        $params = ConnectionParameters::fromString($connectionString);

        self::assertSame($connectionString, $params->connectionString);
    }
}
