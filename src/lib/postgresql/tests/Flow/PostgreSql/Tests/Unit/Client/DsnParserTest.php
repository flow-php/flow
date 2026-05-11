<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client;

use Flow\PostgreSql\Client\DsnParser;
use Flow\PostgreSql\Client\DsnParserException;
use PHPUnit\Framework\TestCase;

final class DsnParserTest extends TestCase
{
    private DsnParser $parser;

    protected function setUp(): void
    {
        $this->parser = new DsnParser();
    }

    public function test_exception_masks_password(): void
    {
        try {
            $this->parser->parse('mysql://user:secretpassword@localhost/db');
            static::fail('Expected DsnParserException');
        } catch (DsnParserException $e) {
            static::assertStringContainsString('***', $e->getMessage());
            static::assertStringNotContainsString('secretpassword', $e->getMessage());
        }
    }

    public function test_parse_basic_dsn(): void
    {
        $params = $this->parser->parse('postgres://localhost/testdb');

        static::assertStringContainsString('host=localhost', $params->toString());
        static::assertStringContainsString('port=5432', $params->toString());
        static::assertStringContainsString('dbname=testdb', $params->toString());
    }

    public function test_parse_dsn_with_credentials(): void
    {
        $params = $this->parser->parse('postgres://admin:secret123@db.example.com:5433/testdb');

        static::assertStringContainsString('host=db.example.com', $params->toString());
        static::assertStringContainsString('port=5433', $params->toString());
        static::assertStringContainsString('dbname=testdb', $params->toString());
        static::assertStringContainsString('user=admin', $params->toString());
        static::assertStringContainsString('password=secret123', $params->toString());
    }

    public function test_parse_dsn_with_custom_port(): void
    {
        $params = $this->parser->parse('postgres://localhost:5433/testdb');

        static::assertStringContainsString('port=5433', $params->toString());
    }

    public function test_parse_dsn_with_default_port(): void
    {
        $params = $this->parser->parse('postgres://localhost/testdb');

        static::assertStringContainsString('port=5432', $params->toString());
    }

    public function test_parse_dsn_with_encoded_credentials(): void
    {
        $params = $this->parser->parse('postgres://user:p%40ss%2Fword@localhost/testdb');

        static::assertStringContainsString('user=user', $params->toString());
        static::assertStringContainsString('password=p@ss/word', $params->toString());
    }

    public function test_parse_dsn_with_encoded_username(): void
    {
        $params = $this->parser->parse('postgres://user%40domain:pass@localhost/testdb');

        static::assertStringContainsString('user=user@domain', $params->toString());
    }

    public function test_parse_dsn_with_multiple_options(): void
    {
        $params = $this->parser->parse(
            'postgres://user:pass@localhost/db?sslmode=verify-full&application_name=myapp&connect_timeout=30',
        );

        static::assertStringContainsString('sslmode=verify-full', $params->toString());
        static::assertStringContainsString('application_name=myapp', $params->toString());
        static::assertStringContainsString('connect_timeout=30', $params->toString());
    }

    public function test_parse_dsn_with_query_options(): void
    {
        $params = $this->parser->parse('postgres://localhost/testdb?sslmode=require&connect_timeout=10');

        static::assertStringContainsString('dbname=testdb', $params->toString());
        static::assertStringContainsString('sslmode=require', $params->toString());
        static::assertStringContainsString('connect_timeout=10', $params->toString());
    }

    public function test_parse_dsn_with_user_only(): void
    {
        $params = $this->parser->parse('postgres://admin@localhost/testdb');

        static::assertStringContainsString('user=admin', $params->toString());
        static::assertStringNotContainsString('password=', $params->toString());
    }

    public function test_parse_dsn_without_user(): void
    {
        $params = $this->parser->parse('postgres://localhost/testdb');

        static::assertStringNotContainsString('user=', $params->toString());
        static::assertStringNotContainsString('password=', $params->toString());
    }

    public function test_parse_pgsql_scheme(): void
    {
        $params = $this->parser->parse('pgsql://user:pass@localhost:5433/mydb');

        static::assertStringContainsString('host=localhost', $params->toString());
        static::assertStringContainsString('port=5433', $params->toString());
        static::assertStringContainsString('dbname=mydb', $params->toString());
        static::assertStringContainsString('user=user', $params->toString());
    }

    public function test_parse_postgresql_scheme(): void
    {
        $params = $this->parser->parse('postgresql://user:pass@localhost/mydb');

        static::assertStringContainsString('host=localhost', $params->toString());
        static::assertStringContainsString('dbname=mydb', $params->toString());
        static::assertStringContainsString('user=user', $params->toString());
        static::assertStringContainsString('password=pass', $params->toString());
    }

    public function test_throws_on_missing_database(): void
    {
        $this->expectException(DsnParserException::class);
        $this->expectExceptionMessage('must specify a database name');

        $this->parser->parse('postgres://localhost/');
    }

    public function test_throws_on_missing_database_without_slash(): void
    {
        $this->expectException(DsnParserException::class);
        $this->expectExceptionMessage('must specify a database name');

        $this->parser->parse('postgres://localhost');
    }

    public function test_throws_on_unsupported_scheme(): void
    {
        $this->expectException(DsnParserException::class);
        $this->expectExceptionMessage('Unsupported DSN scheme');

        $this->parser->parse('mysql://localhost/testdb');
    }
}
