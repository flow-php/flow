<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\schema_function;

use Flow\PostgreSql\Schema\FunctionVolatility;
use PHPUnit\Framework\TestCase;

final class FunctionTest extends TestCase
{
    public function test_function_construction() : void
    {
        $func = schema_function('now', 'timestamp with time zone');

        self::assertSame('now', $func->name);
        self::assertSame('timestamp with time zone', $func->returnType);
        self::assertSame([], $func->argumentTypes);
        self::assertSame('sql', $func->language);
        self::assertNull($func->definition);
        self::assertFalse($func->isStrict);
        self::assertNull($func->volatility);
    }

    public function test_function_with_all_options() : void
    {
        $func = schema_function(
            'add_numbers',
            'integer',
            argumentTypes: ['integer', 'integer'],
            language: 'plpgsql',
            definition: 'BEGIN RETURN $1 + $2; END;',
            isStrict: true,
            volatility: FunctionVolatility::IMMUTABLE,
        );

        self::assertSame(['integer', 'integer'], $func->argumentTypes);
        self::assertSame('plpgsql', $func->language);
        self::assertTrue($func->isStrict);
        self::assertSame(FunctionVolatility::IMMUTABLE, $func->volatility);
    }

    public function test_to_sql_generates_create_function() : void
    {
        self::assertSame(
            'CREATE OR REPLACE FUNCTION add_numbers(IN int, IN int) RETURNS int LANGUAGE plpgsql RETURNS NULL ON NULL INPUT IMMUTABLE AS $$BEGIN RETURN $1 + $2; END;$$',
            schema_function(
                'add_numbers',
                'integer',
                argumentTypes: ['integer', 'integer'],
                language: 'plpgsql',
                definition: 'BEGIN RETURN $1 + $2; END;',
                isStrict: true,
                volatility: FunctionVolatility::IMMUTABLE,
            )->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_function_without_arguments() : void
    {
        self::assertSame(
            'CREATE OR REPLACE FUNCTION current_user_id() RETURNS int LANGUAGE sql AS $$SELECT 1$$',
            schema_function(
                'current_user_id',
                'integer',
                language: 'sql',
                definition: 'SELECT 1',
            )->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_immutable_function_without_strict() : void
    {
        self::assertSame(
            'CREATE OR REPLACE FUNCTION pi_val() RETURNS double precision LANGUAGE sql IMMUTABLE AS $$SELECT 3.14159$$',
            schema_function(
                'pi_val',
                'double precision',
                language: 'sql',
                definition: 'SELECT 3.14159',
                volatility: FunctionVolatility::IMMUTABLE,
            )->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_stable_function() : void
    {
        self::assertSame(
            'CREATE OR REPLACE FUNCTION get_name(IN int) RETURNS text LANGUAGE sql STABLE AS $$SELECT name FROM users WHERE id = $1$$',
            schema_function(
                'get_name',
                'text',
                argumentTypes: ['integer'],
                language: 'sql',
                definition: 'SELECT name FROM users WHERE id = $1',
                volatility: FunctionVolatility::STABLE,
            )->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_strict_function_without_volatility() : void
    {
        self::assertSame(
            'CREATE OR REPLACE FUNCTION double_it(IN int) RETURNS int LANGUAGE sql RETURNS NULL ON NULL INPUT AS $$SELECT $1 * 2$$',
            schema_function(
                'double_it',
                'integer',
                argumentTypes: ['integer'],
                language: 'sql',
                definition: 'SELECT $1 * 2',
                isStrict: true,
            )->toSql()->toSql(),
        );
    }

    public function test_to_sql_generates_volatile_function() : void
    {
        self::assertSame(
            'CREATE OR REPLACE FUNCTION update_ts() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$BEGIN NEW.updated_at = now(); RETURN NEW; END;$$',
            schema_function(
                'update_ts',
                'trigger',
                language: 'plpgsql',
                definition: 'BEGIN NEW.updated_at = now(); RETURN NEW; END;',
                volatility: FunctionVolatility::VOLATILE,
            )->toSql()->toSql(),
        );
    }

    public function test_to_sql_returns_null_when_definition_null() : void
    {
        self::assertNull(
            schema_function('now', 'timestamp with time zone')->toSql(),
        );
    }
}
