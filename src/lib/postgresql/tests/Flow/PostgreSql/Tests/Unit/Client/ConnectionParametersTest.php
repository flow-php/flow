<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client;

use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\DsnParser;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class ConnectionParametersTest extends TestCase
{
    public function test_debug_info_masks_password(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', user: 'admin', password: 'supersecret');

        $debugInfo = $params->__debugInfo();

        static::assertSame('***', $debugInfo['password']);
        static::assertSame('admin', $debugInfo['user']);
        static::assertSame('testdb', $debugInfo['database']);
    }

    public function test_debug_info_shows_null_for_no_password(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb');

        $debugInfo = $params->__debugInfo();

        static::assertNull($debugInfo['password']);
    }

    public function test_from_params_basic(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb');

        static::assertStringContainsString('host=localhost', $params->toString());
        static::assertStringContainsString('port=5432', $params->toString());
        static::assertStringContainsString('dbname=testdb', $params->toString());
    }

    public function test_from_params_returns_correct_getters(): void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
            host: 'db.example.com',
            port: 5433,
            user: 'admin',
            password: 'secret123',
            options: ['sslmode' => 'require'],
        );

        static::assertSame('db.example.com', $params->host());
        static::assertSame(5433, $params->port());
        static::assertSame('testdb', $params->database());
        static::assertSame('admin', $params->user());
        static::assertSame('secret123', $params->password());
        static::assertSame(['sslmode' => 'require'], $params->options());
    }

    public function test_from_params_with_credentials(): void
    {
        $params = ConnectionParameters::fromParams(
            database: 'testdb',
            host: 'db.example.com',
            port: 5433,
            user: 'admin',
            password: 'secret123',
        );

        static::assertStringContainsString('host=db.example.com', $params->toString());
        static::assertStringContainsString('port=5433', $params->toString());
        static::assertStringContainsString('dbname=testdb', $params->toString());
        static::assertStringContainsString('user=admin', $params->toString());
        static::assertStringContainsString('password=secret123', $params->toString());
    }

    public function test_from_params_with_options(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', options: [
            'sslmode' => 'require',
            'connect_timeout' => '10',
        ]);

        static::assertStringContainsString('sslmode=require', $params->toString());
        static::assertStringContainsString('connect_timeout=10', $params->toString());
    }

    public function test_from_params_without_optional_credentials(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb');

        static::assertStringNotContainsString('user=', $params->toString());
        static::assertStringNotContainsString('password=', $params->toString());
        static::assertNull($params->user());
        static::assertNull($params->password());
    }

    public function test_from_string(): void
    {
        $connectionString = 'host=localhost port=5432 dbname=test user=postgres password=secret';

        $params = ConnectionParameters::fromString($connectionString);

        static::assertSame('localhost', $params->host());
        static::assertSame(5432, $params->port());
        static::assertSame('test', $params->database());
        static::assertSame('postgres', $params->user());
        static::assertSame('secret', $params->password());
    }

    public function test_from_string_throws_exception_when_dbname_missing(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Missing dbname in connection string');

        ConnectionParameters::fromString('host=localhost port=5432');
    }

    public function test_from_string_with_defaults(): void
    {
        $params = ConnectionParameters::fromString('dbname=test');

        static::assertSame('localhost', $params->host());
        static::assertSame(5432, $params->port());
        static::assertSame('test', $params->database());
        static::assertNull($params->user());
        static::assertNull($params->password());
    }

    public function test_from_string_with_options(): void
    {
        $connectionString = 'host=localhost port=5432 dbname=test sslmode=require connect_timeout=10';

        $params = ConnectionParameters::fromString($connectionString);

        static::assertSame(['sslmode' => 'require', 'connect_timeout' => '10'], $params->options());
    }

    public function test_from_string_with_quoted_password(): void
    {
        $connectionString = "host=localhost dbname=test password='my secret pass'";

        $params = ConnectionParameters::fromString($connectionString);

        static::assertSame('my secret pass', $params->password());
    }

    public function test_to_string_roundtrip(): void
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

        static::assertSame($original->host(), $parsed->host());
        static::assertSame($original->port(), $parsed->port());
        static::assertSame($original->database(), $parsed->database());
        static::assertSame($original->user(), $parsed->user());
        static::assertSame($original->password(), $parsed->password());
        static::assertSame($original->options(), $parsed->options());
    }

    public function test_with_database(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb');

        $modified = $params->withDatabase('testdb_test');

        static::assertSame('testdb', $params->database());
        static::assertSame('testdb_test', $modified->database());
        static::assertNotSame($params, $modified);
    }

    public function test_with_database_allows_appending_suffix(): void
    {
        $params = ConnectionParameters::fromParams(database: 'myapp');

        $testParams = $params->withDatabase($params->database() . '_test');

        static::assertSame('myapp_test', $testParams->database());
    }

    public function test_with_database_suffix_appends_to_database(): void
    {
        $params = ConnectionParameters::fromParams(database: 'app');

        $modified = $params->withDatabaseSuffix('_test');

        static::assertSame('app', $params->database());
        static::assertSame('app_test', $modified->database());
        static::assertNotSame($params, $modified);
    }

    public function test_with_database_suffix_composes_after_with_database(): void
    {
        $params = ConnectionParameters::fromParams(database: 'app');

        $modified = $params->withDatabase('other')->withDatabaseSuffix('_test');

        static::assertSame('other_test', $modified->database());
    }

    public function test_with_database_suffix_empty_is_no_op(): void
    {
        $params = ConnectionParameters::fromParams(database: 'app');

        $modified = $params->withDatabaseSuffix('');

        static::assertSame($params, $modified);
        static::assertSame('app', $modified->database());
    }

    public function test_with_database_suffix_round_trips_with_dsn_parser(): void
    {
        $params = (new DsnParser())
            ->parse('postgresql://user:pass@localhost:5432/mydb')
            ->withDatabaseSuffix('_x');

        static::assertSame('mydb_x', $params->database());
    }

    public function test_with_host(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', host: 'localhost');

        $modified = $params->withHost('db.production.com');

        static::assertSame('localhost', $params->host());
        static::assertSame('db.production.com', $modified->host());
    }

    public function test_with_option(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb');

        $modified = $params->withOption('sslmode', 'require');

        static::assertSame([], $params->options());
        static::assertSame(['sslmode' => 'require'], $modified->options());
    }

    public function test_with_option_adds_to_existing(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', options: ['sslmode' => 'require']);

        $modified = $params->withOption('connect_timeout', '10');

        static::assertSame(['sslmode' => 'require'], $params->options());
        static::assertSame(['sslmode' => 'require', 'connect_timeout' => '10'], $modified->options());
    }

    public function test_with_options_replaces_all(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', options: [
            'sslmode' => 'require',
            'connect_timeout' => '10',
        ]);

        $modified = $params->withOptions(['application_name' => 'myapp']);

        static::assertSame(['sslmode' => 'require', 'connect_timeout' => '10'], $params->options());
        static::assertSame(['application_name' => 'myapp'], $modified->options());
    }

    public function test_with_password(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', password: 'secret');

        $modified = $params->withPassword('newsecret');

        static::assertSame('secret', $params->password());
        static::assertSame('newsecret', $modified->password());
    }

    public function test_with_port(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', port: 5432);

        $modified = $params->withPort(5433);

        static::assertSame(5432, $params->port());
        static::assertSame(5433, $modified->port());
    }

    public function test_with_user(): void
    {
        $params = ConnectionParameters::fromParams(database: 'testdb', user: 'admin');

        $modified = $params->withUser('readonly');

        static::assertSame('admin', $params->user());
        static::assertSame('readonly', $modified->user());
    }
}
