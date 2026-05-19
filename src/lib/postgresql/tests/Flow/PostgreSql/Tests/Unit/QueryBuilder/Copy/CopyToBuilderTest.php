<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Copy;

use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\CopyStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RawStmt;
use Flow\PostgreSql\QueryBuilder\Copy\CopyFormat;
use Flow\PostgreSql\QueryBuilder\Copy\CopyToBuilder;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function function_exists;

final class CopyToBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_builder_steps_allow_fluent_interface(): void
    {
        $query = CopyToBuilder::create()->table('users')->file('/tmp/users.csv')->format(CopyFormat::CSV)->withHeader();

        $ast = $query->toAst();
        static::assertInstanceOf(CopyStmt::class, $ast);
    }

    public function test_copy_to_basic_table(): void
    {
        $query = CopyToBuilder::create()->table('users')->file('/tmp/users.csv');

        $ast = $query->toAst();

        static::assertFalse($ast->getIsFrom());
        static::assertFalse($ast->getIsProgram());
        static::assertSame('/tmp/users.csv', $ast->getFilename());

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
    }

    public function test_copy_to_deparsed_basic_table(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users')->file('/tmp/users.csv');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.csv'", $deparsed);
    }

    public function test_copy_to_deparsed_binary_format(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users')->file('/tmp/users.bin')->format(CopyFormat::BINARY);

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.bin' WITH (FORMAT BINARY)", $deparsed);
    }

    public function test_copy_to_deparsed_from_query(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $selectQuery = SelectBuilder::create()
            ->select(Column::name('id'), Column::name('name'))
            ->from(new Table('users'));

        $query = CopyToBuilder::create()->query($selectQuery)->file('/tmp/users.csv');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY (SELECT id, name FROM users) TO '/tmp/users.csv'", $deparsed);
    }

    public function test_copy_to_deparsed_to_program(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users')->program('gzip > /tmp/users.csv.gz');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO PROGRAM 'gzip > /tmp/users.csv.gz'", $deparsed);
    }

    public function test_copy_to_deparsed_to_stdout(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users')->stdout();

        $deparsed = $this->deparse($query->toAst());
        static::assertSame('COPY users TO STDOUT', $deparsed);
    }

    public function test_copy_to_deparsed_with_columns(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users', 'id', 'name', 'email')->file('/tmp/users.csv');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users(id, name, email) TO '/tmp/users.csv'", $deparsed);
    }

    public function test_copy_to_deparsed_with_csv_format(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users')->file('/tmp/users.csv')->format(CopyFormat::CSV);

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.csv' CSV", $deparsed);
    }

    public function test_copy_to_deparsed_with_delimiter(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()
            ->table('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->delimiter(';');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.csv' WITH (FORMAT CSV, DELIMITER ';')", $deparsed);
    }

    public function test_copy_to_deparsed_with_encoding(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users')->file('/tmp/users.csv')->encoding('UTF8');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.csv' WITH (ENCODING 'UTF8')", $deparsed);
    }

    public function test_copy_to_deparsed_with_escape(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users')->file('/tmp/users.csv')->format(CopyFormat::CSV)->escape('\\');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.csv' WITH (FORMAT CSV, ESCAPE E'\\\\')", $deparsed);
    }

    public function test_copy_to_deparsed_with_force_quote_all(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()
            ->table('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->forceQuoteAll();

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.csv' WITH (FORMAT CSV, FORCE_QUOTE *)", $deparsed);
    }

    public function test_copy_to_deparsed_with_force_quote_columns(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()
            ->table('users')
            ->file('/tmp/users.csv')
            ->format(CopyFormat::CSV)
            ->forceQuote('name', 'email');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.csv' CSV FORCE QUOTE name, email", $deparsed);
    }

    public function test_copy_to_deparsed_with_header(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users')->file('/tmp/users.csv')->format(CopyFormat::CSV)->withHeader();

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.csv' WITH (FORMAT CSV, HEADER true)", $deparsed);
    }

    public function test_copy_to_deparsed_with_null_string(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users')->file('/tmp/users.csv')->nullAs('\\N');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.csv' WITH (NULL E'\\\\N')", $deparsed);
    }

    public function test_copy_to_deparsed_with_quote(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('users')->file('/tmp/users.csv')->format(CopyFormat::CSV)->quote("'");

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY users TO '/tmp/users.csv' WITH (FORMAT CSV, QUOTE '''')", $deparsed);
    }

    public function test_copy_to_deparsed_with_schema(): void
    {
        if (!function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CopyToBuilder::create()->table('myschema.users')->file('/tmp/users.csv');

        $deparsed = $this->deparse($query->toAst());
        static::assertSame("COPY myschema.users TO '/tmp/users.csv'", $deparsed);
    }

    public function test_copy_to_parses_schema_and_table(): void
    {
        $query = CopyToBuilder::create()->table('myschema.users')->file('/tmp/users.csv');

        $ast = $query->toAst();

        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('users', $relation->getRelname());
        static::assertSame('myschema', $relation->getSchemaname());
    }

    public function test_copy_to_with_query_has_no_relation(): void
    {
        $selectQuery = SelectBuilder::create()->select(Column::name('id'))->from(new Table('users'));

        $query = CopyToBuilder::create()->query($selectQuery)->file('/tmp/users.csv');

        $ast = $query->toAst();

        static::assertFalse($ast->hasRelation());
        static::assertTrue($ast->hasQuery());
    }

    public function test_immutability_destination(): void
    {
        $original = CopyToBuilder::create()->table('users');
        $modified = $original->file('/tmp/users.csv');

        static::assertNotSame($original, $modified);
    }

    public function test_immutability_options(): void
    {
        $original = CopyToBuilder::create()->table('users')->file('/tmp/users.csv');
        $modified = $original->format(CopyFormat::CSV);

        static::assertNotSame($original, $modified);
    }

    public function test_immutability_table(): void
    {
        $original = CopyToBuilder::create();
        $modified = $original->table('users');

        static::assertNotSame($original, $modified);
    }

    public function test_to_ast_without_destination_throws_exception(): void
    {
        $builder = CopyToBuilder::create()->table('users');
        static::assertInstanceOf(CopyToBuilder::class, $builder);

        $this->expectException(InvalidExpressionException::class);

        $builder->toAst();
    }

    public function test_to_ast_without_table_or_query_throws_exception(): void
    {
        $builder = CopyToBuilder::create();
        static::assertInstanceOf(CopyToBuilder::class, $builder);
        $withFile = $builder->file('/tmp/users.csv');
        static::assertInstanceOf(CopyToBuilder::class, $withFile);

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
