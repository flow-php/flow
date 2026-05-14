<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Copy;

use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\CopyStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RawStmt;
use Flow\PostgreSql\QueryBuilder\Copy\CopyFormat;
use Flow\PostgreSql\QueryBuilder\Copy\CopyFromBuilder;
use Flow\PostgreSql\QueryBuilder\Copy\CopyOnError;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use PHPUnit\Framework\TestCase;

final class CopyFromBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_builder_steps_allow_fluent_interface(): void
    {
        $query = CopyFromBuilder::create()
            ->table('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->withHeader();

        $ast = $query->toAst();
        static::assertInstanceOf(CopyStmt::class, $ast);
    }

    public function test_copy_format_enum_values(): void
    {
        static::assertSame('binary', CopyFormat::BINARY->value);
        static::assertSame('csv', CopyFormat::CSV->value);
        static::assertSame('text', CopyFormat::TEXT->value);
    }

    public function test_copy_from_basic_table(): void
    {
        $query = CopyFromBuilder::create()->table('users')->file('/tmp/users.csv');

        $ast = $query->toAst();

        static::assertTrue($ast->getIsFrom());
        static::assertFalse($ast->getIsProgram());
        static::assertSame('/tmp/users.csv', $ast->getFilename());

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
    }

    public function test_copy_from_deparsed_basic_table(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('users')->file('/tmp/users.csv');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.csv'", $deparsed);
    }

    public function test_copy_from_deparsed_binary_format(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('users')->file('/tmp/users.bin')->format(CopyFormat::BINARY);

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.bin' WITH (FORMAT BINARY)", $deparsed);
    }

    public function test_copy_from_deparsed_from_program(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('users')->program('gunzip -c /tmp/users.csv.gz');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM PROGRAM 'gunzip -c /tmp/users.csv.gz'", $deparsed);
    }

    public function test_copy_from_deparsed_from_stdin(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('users')->stdin();

        $deparsed = $this->deparse($query->toAst());
        static::assertSame('COPY users FROM STDIN', $deparsed);
    }

    public function test_copy_from_deparsed_with_columns(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('users', 'id', 'name', 'email')->file('/tmp/users.csv');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users(id, name, email) FROM '/tmp/users.csv'", $deparsed);
    }

    public function test_copy_from_deparsed_with_csv_format(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('users')->file('/tmp/users.csv')->format(CopyFormat::CSV);

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.csv' CSV", $deparsed);
    }

    public function test_copy_from_deparsed_with_delimiter(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->delimiter(';');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, DELIMITER ';')", $deparsed);
    }

    public function test_copy_from_deparsed_with_encoding(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('users')->file('/tmp/users.csv')->encoding('UTF8');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.csv' WITH (ENCODING 'UTF8')", $deparsed);
    }

    public function test_copy_from_deparsed_with_escape(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->escape('\\');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, ESCAPE E'\\\\')", $deparsed);
    }

    public function test_copy_from_deparsed_with_force_not_null(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->forceNotNull('name', 'email');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame(
            "COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, FORCE_NOT_NULL (name, email))",
            $deparsed,
        );
    }

    public function test_copy_from_deparsed_with_force_null(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->forceNull('name', 'email');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, FORCE_NULL (name, email))", $deparsed);
    }

    public function test_copy_from_deparsed_with_header(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()
            ->table('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->withHeader();

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, HEADER true)", $deparsed);
    }

    public function test_copy_from_deparsed_with_null_string(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('users')->file('/tmp/users.csv')->nullAs('\\N');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.csv' WITH (NULL E'\\\\N')", $deparsed);
    }

    public function test_copy_from_deparsed_with_on_error_ignore(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('users')->file('/tmp/users.csv')->onError(CopyOnError::IGNORE);

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.csv' WITH (on_error ignore)", $deparsed);
    }

    public function test_copy_from_deparsed_with_quote(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('users')->file('/tmp/users.csv')->format(CopyFormat::CSV)->quote("'");

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users FROM '/tmp/users.csv' WITH (FORMAT CSV, QUOTE '''')", $deparsed);
    }

    public function test_copy_from_deparsed_with_schema(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyFromBuilder::create()->table('myschema.users')->file('/tmp/users.csv');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY myschema.users FROM '/tmp/users.csv'", $deparsed);
    }

    public function test_copy_from_parses_schema_and_table(): void
    {
        $query = CopyFromBuilder::create()->table('myschema.users')->file('/tmp/users.csv');

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
        static::assertSame('myschema', $relation->getSchemaname());
    }

    public function test_copy_on_error_enum_values(): void
    {
        static::assertSame('ignore', CopyOnError::IGNORE->value);
        static::assertSame('stop', CopyOnError::STOP->value);
    }

    public function test_immutability_options(): void
    {
        $original = CopyFromBuilder::create()->table('users')->file('/tmp/users.csv');
        $modified = $original->format(CopyFormat::CSV);

        static::assertNotSame($original, $modified);
    }

    public function test_immutability_source(): void
    {
        $original = CopyFromBuilder::create()->table('users');
        $modified = $original->file('/tmp/users.csv');

        static::assertNotSame($original, $modified);
    }

    public function test_immutability_table(): void
    {
        $original = CopyFromBuilder::create();
        $modified = $original->table('users');

        static::assertNotSame($original, $modified);
    }

    public function test_to_ast_without_source_throws_exception(): void
    {
        $builder = CopyFromBuilder::create()->table('users');
        static::assertInstanceOf(CopyFromBuilder::class, $builder);

        $this->expectException(InvalidExpressionException::class);

        $builder->toAst();
    }

    public function test_to_ast_without_table_throws_exception(): void
    {
        $builder = CopyFromBuilder::create();
        static::assertInstanceOf(CopyFromBuilder::class, $builder);
        $withFile = $builder->file('/tmp/users.csv');
        static::assertInstanceOf(CopyFromBuilder::class, $withFile);

        $this->expectException(InvalidExpressionException::class);

        $withFile->toAst();
    }

    private function deparse(CopyStmt $copyStmt): string
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
