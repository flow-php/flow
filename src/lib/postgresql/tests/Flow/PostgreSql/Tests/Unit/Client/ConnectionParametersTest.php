<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client;

use Flow\PostgreSql\Client\ConnectionParameters;
use PHPUnit\Framework\TestCase;

final class ConnectionParametersTest extends TestCase
{
    public function test_debug_info_masks_password() : void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
            user: 'admin',
            password: 'supersecret',
        );

        $debugInfo = $params->__debugInfo();

        self::assertSame('***', $debugInfo['password']);
        self::assertSame('admin', $debugInfo['user']);
        self::assertSame('testdb', $debugInfo['database']);
    }

    public function test_debug_info_shows_null_for_no_password() : void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb');

        $debugInfo = $params->__debugInfo();

        self::assertNull($debugInfo['password']);
    }

    public function test_from_params_basic() : void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
        );

        self::assertStringContainsString('host=localhost', $params->toString());
        self::assertStringContainsString('port=5432', $params->toString());
        self::assertStringContainsString('dbname=testdb', $params->toString());
    }

    public function test_from_params_returns_correct_getters() : void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
            host: 'db.example.com',
            port: 5433,
            user: 'admin',
            password: 'secret123',
            options: ['sslmode' => 'require'],
        );

        self::assertSame('db.example.com', $params->host());
        self::assertSame(5433, $params->port());
        self::assertSame('testdb', $params->database());
        self::assertSame('admin', $params->user());
        self::assertSame('secret123', $params->password());
        self::assertSame(['sslmode' => 'require'], $params->options());
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

        self::assertStringContainsString('host=db.example.com', $params->toString());
        self::assertStringContainsString('port=5433', $params->toString());
        self::assertStringContainsString('dbname=testdb', $params->toString());
        self::assertStringContainsString('user=admin', $params->toString());
        self::assertStringContainsString('password=secret123', $params->toString());
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

        self::assertStringContainsString('sslmode=require', $params->toString());
        self::assertStringContainsString('connect_timeout=10', $params->toString());
    }

    public function test_from_params_without_optional_credentials() : void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
        );

        self::assertStringNotContainsString('user=', $params->toString());
        self::assertStringNotContainsString('password=', $params->toString());
        self::assertNull($params->user());
        self::assertNull($params->password());
    }

    public function test_from_string() : void
    {
        $connectionString = 'host=localhost port=5432 dbname=test user=postgres password=secret';

        $params = ConnectionParameters::fromString($connectionString);

        self::assertSame('localhost', $params->host());
        self::assertSame(5432, $params->port());
        self::assertSame('test', $params->database());
        self::assertSame('postgres', $params->user());
        self::assertSame('secret', $params->password());
    }

    public function test_from_string_throws_exception_when_dbname_missing() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Missing dbname in connection string');

        ConnectionParameters::fromString('host=localhost port=5432');
    }

    public function test_from_string_with_defaults() : void
    {
        $params = ConnectionParameters::fromString('dbname=test');

        self::assertSame('localhost', $params->host());
        self::assertSame(5432, $params->port());
        self::assertSame('test', $params->database());
        self::assertNull($params->user());
        self::assertNull($params->password());
    }

    public function test_from_string_with_options() : void
    {
        $connectionString = 'host=localhost port=5432 dbname=test sslmode=require connect_timeout=10';

        $params = ConnectionParameters::fromString($connectionString);

        self::assertSame(['sslmode' => 'require', 'connect_timeout' => '10'], $params->options());
    }

    public function test_from_string_with_quoted_password() : void
    {
        $connectionString = "host=localhost dbname=test password='my secret pass'";

        $params = ConnectionParameters::fromString($connectionString);

        self::assertSame('my secret pass', $params->password());
    }

    public function test_to_string_roundtrip() : void
    {
        $original = ConnectionParameters::fromParams(
            database: 'testdb',
            host: 'db.example.com',
            port: 5433,
            user: 'admin',
            password: 'secret',
            options: ['sslmode' => 'require'],
        );

        $connectionString = $original->toString();
        $parsed = ConnectionParameters::fromString($connectionString);

        self::assertSame($original->host(), $parsed->host());
        self::assertSame($original->port(), $parsed->port());
        self::assertSame($original->database(), $parsed->database());
        self::assertSame($original->user(), $parsed->user());
        self::assertSame($original->password(), $parsed->password());
        self::assertSame($original->options(), $parsed->options());
    }

    public function test_with_database() : void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb');

        $modified = $params->withDatabase('testdb_test');

        self::assertSame('testdb', $params->database());
        self::assertSame('testdb_test', $modified->database());
        self::assertNotSame($params, $modified);
    }

    public function test_with_database_allows_appending_suffix() : void
    {
        $params = ConnectionParameters::fromParams(database: 'myapp');

        $testParams = $params->withDatabase($params->database() . '_test');

        self::assertSame('myapp_test', $testParams->database());
    }

    public function test_with_host() : void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', host: 'localhost');

        $modified = $params->withHost('db.production.com');

        self::assertSame('localhost', $params->host());
        self::assertSame('db.production.com', $modified->host());
    }

    public function test_with_option() : void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb');

        $modified = $params->withOption('sslmode', 'require');

        self::assertSame([], $params->options());
        self::assertSame(['sslmode' => 'require'], $modified->options());
    }

    public function test_with_option_adds_to_existing() : void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
            options: ['sslmode' => 'require'],
        );

        $modified = $params->withOption('connect_timeout', '10');

        self::assertSame(['sslmode' => 'require'], $params->options());
        self::assertSame(['sslmode' => 'require', 'connect_timeout' => '10'], $modified->options());
    }

    public function test_with_options_replaces_all() : void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
            options: ['sslmode' => 'require', 'connect_timeout' => '10'],
        );

        $modified = $params->withOptions(['application_name' => 'myapp']);

        self::assertSame(['sslmode' => 'require', 'connect_timeout' => '10'], $params->options());
        self::assertSame(['application_name' => 'myapp'], $modified->options());
    }

    public function test_with_password() : void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', password: 'secret');

        $modified = $params->withPassword('newsecret');

        self::assertSame('secret', $params->password());
        self::assertSame('newsecret', $modified->password());
    }

    public function test_with_port() : void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', port: 5432);

        $modified = $params->withPort(5433);

        self::assertSame(5432, $params->port());
        self::assertSame(5433, $modified->port());
    }

    public function test_with_user() : void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', user: 'admin');

        $modified = $params->withUser('readonly');

        self::assertSame('admin', $params->user());
        self::assertSame('readonly', $modified->user());
    }
}
