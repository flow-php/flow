<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Copy;

use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\PgQuery\Protobuf\AST\{CopyStmt, Node, RawStmt};
use Flow\PgQuery\QueryBuilder\Copy\{CopyFormat, CopyFromBuilder, CopyOnError};
use Flow\PgQuery\QueryBuilder\Exception\InvalidExpressionException;
use PHPUnit\Framework\TestCase;

final class CopyFromBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_builder_steps_allow_fluent_interface() : void
    {
        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->withHeader();

        $ast = $query->toAst();
        self::assertInstanceOf(CopyStmt::class, $ast);
    }

    public function test_copy_format_enum_values() : void
    {
        self::assertSame('binary', CopyFormat::BINARY->value);
        self::assertSame('csv', CopyFormat::CSV->value);
        self::assertSame('text', CopyFormat::TEXT->value);
    }

    public function test_copy_from_basic_table() : void
    {
        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv');

        $ast = $query->toAst();

        self::assertTrue($ast->getIsFrom());
        self::assertFalse($ast->getIsProgram());
        self::assertSame('/tmp/users.csv', $ast->getFilename());

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
    }

    public function test_copy_from_deparsed_basic_table() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv'", $deparsed);
    }

    public function test_copy_from_deparsed_binary_format() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.bin')
            ->format(CopyFormat::BINARY);

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.bin' WITH (FORMAT BINARY)", $deparsed);
    }

    public function test_copy_from_deparsed_from_program() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromProgram('gunzip -c /tmp/users.csv.gz');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM PROGRAM 'gunzip -c /tmp/users.csv.gz'", $deparsed);
    }

    public function test_copy_from_deparsed_from_stdin() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromStdin();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('COPY users FROM STDIN', $deparsed);
    }

    public function test_copy_from_deparsed_with_columns() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users', 'id', 'name', 'email')
            ->fromFile('/tmp/users.csv');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users(id, name, email) FROM '/tmp/users.csv'", $deparsed);
    }

    public function test_copy_from_deparsed_with_csv_format() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->format(CopyFormat::CSV);

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv' CSV", $deparsed);
    }

    public function test_copy_from_deparsed_with_delimiter() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->delimiter(';');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, DELIMITER ';')", $deparsed);
    }

    public function test_copy_from_deparsed_with_encoding() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->encoding('UTF8');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (ENCODING 'UTF8')", $deparsed);
    }

    public function test_copy_from_deparsed_with_escape() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->escape('\\');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, ESCAPE E'\\\\')", $deparsed);
    }

    public function test_copy_from_deparsed_with_force_not_null() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->forceNotNull('name', 'email');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, FORCE_NOT_NULL (name, email))", $deparsed);
    }

    public function test_copy_from_deparsed_with_force_null() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->forceNull('name', 'email');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, FORCE_NULL (name, email))", $deparsed);
    }

    public function test_copy_from_deparsed_with_header() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->withHeader();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, HEADER true)", $deparsed);
    }

    public function test_copy_from_deparsed_with_null_string() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->nullAs('\\N');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (NULL E'\\\\N')", $deparsed);
    }

    public function test_copy_from_deparsed_with_on_error_ignore() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->onError(CopyOnError::IGNORE);

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (on_error ignore)", $deparsed);
    }

    public function test_copy_from_deparsed_with_quote() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->fromFile('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->quote("'");

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, QUOTE '''')", $deparsed);
    }

    public function test_copy_from_deparsed_with_schema() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('myschema.users')
            ->fromFile('/tmp/users.csv');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COPY myschema.users FROM '/tmp/users.csv'", $deparsed);
    }

    public function test_copy_from_parses_schema_and_table() : void
    {
        $query = CopyFromBuilder::create()
            ->table('myschema.users')
            ->fromFile('/tmp/users.csv');

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        self::assertNotNull($relation);
        self::assertSame('users', $relation->getRelname());
        self::assertSame('myschema', $relation->getSchemaname());
    }

    public function test_copy_on_error_enum_values() : void
    {
        self::assertSame('ignore', CopyOnError::IGNORE->value);
        self::assertSame('stop', CopyOnError::STOP->value);
    }

    public function test_immutability_options() : void
    {
        $original = CopyFromBuilder::create()->table('users')->fromFile('/tmp/users.csv');
        $modified = $original->format(CopyFormat::CSV);

        self::assertNotSame($original, $modified);
    }

    public function test_immutability_source() : void
    {
        $original = CopyFromBuilder::create()->table('users');
        $modified = $original->fromFile('/tmp/users.csv');

        self::assertNotSame($original, $modified);
    }

    public function test_immutability_table() : void
    {
        $original = CopyFromBuilder::create();
        $modified = $original->table('users');

        self::assertNotSame($original, $modified);
    }

    public function test_to_ast_without_source_throws_exception() : void
    {
        $this->expectException(InvalidExpressionException::class);

        CopyFromBuilder::create()
            ->table('users')
            ->toAst();
    }

    public function test_to_ast_without_table_throws_exception() : void
    {
        $this->expectException(InvalidExpressionException::class);

        CopyFromBuilder::create()
            ->fromFile('/tmp/users.csv')
            ->toAst();
    }

    private function deparse(CopyStmt $copyStmt) : string
    {
        $parser = new Parser();
        $node = new Node();
        $node->setCopyStmt($copyStmt);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
