<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client;

use Flow\PostgreSql\Client\{DsnParser, DsnParserException};
use PHPUnit\Framework\TestCase;

final class DsnParserTest extends TestCase
{
    private DsnParser $parser;

    protected function setUp() : void
    {
        $this->parser = new DsnParser();
    }

    public function test_exception_masks_password() : void
    {
        try {
            $this->parser->parse('mysql://user:secretpassword@localhost/db');
            self::fail('Expected DsnParserException');
        } catch (DsnParserException $e) {
            self::assertStringContainsString('***', $e->getMessage());
            self::assertStringNotContainsString('secretpassword', $e->getMessage());
        }
    }

    public function test_parse_basic_dsn() : void
    {
        $params = $this->parser->parse('postgres://localhost/testdb');

        self::assertStringContainsString('host=localhost', $params->connectionString);
        self::assertStringContainsString('port=5432', $params->connectionString);
        self::assertStringContainsString('dbname=testdb', $params->connectionString);
    }

    public function test_parse_dsn_with_credentials() : void
    {
        $params = $this->parser->parse('postgres://admin:secret123@db.example.com:5433/testdb');

        self::assertStringContainsString('host=db.example.com', $params->connectionString);
        self::assertStringContainsString('port=5433', $params->connectionString);
        self::assertStringContainsString('dbname=testdb', $params->connectionString);
        self::assertStringContainsString('user=admin', $params->connectionString);
        self::assertStringContainsString('password=secret123', $params->connectionString);
    }

    public function test_parse_dsn_with_custom_port() : void
    {
        $params = $this->parser->parse('postgres://localhost:5433/testdb');

        self::assertStringContainsString('port=5433', $params->connectionString);
    }

    public function test_parse_dsn_with_default_port() : void
    {
        $params = $this->parser->parse('postgres://localhost/testdb');

        self::assertStringContainsString('port=5432', $params->connectionString);
    }

    public function test_parse_dsn_with_encoded_credentials() : void
    {
        $params = $this->parser->parse('postgres://user:p%40ss%2Fword@localhost/testdb');

        self::assertStringContainsString('user=user', $params->connectionString);
        self::assertStringContainsString('password=p@ss/word', $params->connectionString);
    }

    public function test_parse_dsn_with_encoded_username() : void
    {
        $params = $this->parser->parse('postgres://user%40domain:pass@localhost/testdb');

        self::assertStringContainsString('user=user@domain', $params->connectionString);
    }

    public function test_parse_dsn_with_multiple_options() : void
    {
        $params = $this->parser->parse(
            'postgres://user:pass@localhost/db?sslmode=verify-full&application_name=myapp&connect_timeout=30'
        );

        self::assertStringContainsString('sslmode=verify-full', $params->connectionString);
        self::assertStringContainsString('application_name=myapp', $params->connectionString);
        self::assertStringContainsString('connect_timeout=30', $params->connectionString);
    }

    public function test_parse_dsn_with_query_options() : void
    {
        $params = $this->parser->parse('postgres://localhost/testdb?sslmode=require&connect_timeout=10');

        self::assertStringContainsString('dbname=testdb', $params->connectionString);
        self::assertStringContainsString('sslmode=require', $params->connectionString);
        self::assertStringContainsString('connect_timeout=10', $params->connectionString);
    }

    public function test_parse_dsn_with_user_only() : void
    {
        $params = $this->parser->parse('postgres://admin@localhost/testdb');

        self::assertStringContainsString('user=admin', $params->connectionString);
        self::assertStringNotContainsString('password=', $params->connectionString);
    }

    public function test_parse_dsn_without_user() : void
    {
        $params = $this->parser->parse('postgres://localhost/testdb');

        self::assertStringNotContainsString('user=', $params->connectionString);
        self::assertStringNotContainsString('password=', $params->connectionString);
    }

    public function test_parse_pgsql_scheme() : void
    {
        $params = $this->parser->parse('pgsql://user:pass@localhost:5433/mydb');

        self::assertStringContainsString('host=localhost', $params->connectionString);
        self::assertStringContainsString('port=5433', $params->connectionString);
        self::assertStringContainsString('dbname=mydb', $params->connectionString);
        self::assertStringContainsString('user=user', $params->connectionString);
    }

    public function test_parse_postgresql_scheme() : void
    {
        $params = $this->parser->parse('postgresql://user:pass@localhost/mydb');

        self::assertStringContainsString('host=localhost', $params->connectionString);
        self::assertStringContainsString('dbname=mydb', $params->connectionString);
        self::assertStringContainsString('user=user', $params->connectionString);
        self::assertStringContainsString('password=pass', $params->connectionString);
    }

    public function test_throws_on_missing_database() : void
    {
        $this->expectException(DsnParserException::class);
        $this->expectExceptionMessage('must specify a database name');

        $this->parser->parse('postgres://localhost/');
    }

    public function test_throws_on_missing_database_without_slash() : void
    {
        $this->expectException(DsnParserException::class);
        $this->expectExceptionMessage('must specify a database name');

        $this->parser->parse('postgres://localhost');
    }

    public function test_throws_on_unsupported_scheme() : void
    {
        $this->expectException(DsnParserException::class);
        $this->expectExceptionMessage('Unsupported DSN scheme');

        $this->parser->parse('mysql://localhost/testdb');
    }
}
