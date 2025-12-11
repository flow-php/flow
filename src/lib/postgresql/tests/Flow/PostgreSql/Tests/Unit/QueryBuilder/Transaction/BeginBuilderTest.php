<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Transaction;

use Flow\PostgreSql\{ParsedQuery, Parser};
use Flow\PostgreSql\Protobuf\AST\{Node, RawStmt, TransactionStmt, TransactionStmtKind};
use Flow\PostgreSql\QueryBuilder\Transaction\{BeginBuilder, IsolationLevel};
use PHPUnit\Framework\TestCase;

final class BeginBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_begin_basic() : void
    {
        $query = BeginBuilder::create();

        $ast = $query->toAst();

        self::assertInstanceOf(TransactionStmt::class, $ast);
        self::assertSame(TransactionStmtKind::TRANS_STMT_BEGIN, $ast->getKind());
        self::assertCount(0, $ast->getOptions());
    }

    public function test_begin_deparsed_basic() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = BeginBuilder::create();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('BEGIN', $deparsed);
    }

    public function test_begin_deparsed_with_deferrable() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = BeginBuilder::create()
            ->deferrable();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('BEGIN DEFERRABLE', $deparsed);
    }

    public function test_begin_deparsed_with_isolation_level_read_committed() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = BeginBuilder::create()
            ->isolationLevel(IsolationLevel::READ_COMMITTED);

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('BEGIN ISOLATION LEVEL READ COMMITTED', $deparsed);
    }

    public function test_begin_deparsed_with_isolation_level_repeatable_read() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = BeginBuilder::create()
            ->isolationLevel(IsolationLevel::REPEATABLE_READ);

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('BEGIN ISOLATION LEVEL REPEATABLE READ', $deparsed);
    }

    public function test_begin_deparsed_with_isolation_level_serializable() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = BeginBuilder::create()
            ->isolationLevel(IsolationLevel::SERIALIZABLE);

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('BEGIN ISOLATION LEVEL SERIALIZABLE', $deparsed);
    }

    public function test_begin_deparsed_with_multiple_options() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = BeginBuilder::create()
            ->isolationLevel(IsolationLevel::SERIALIZABLE)
            ->readOnly()
            ->deferrable();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE', $deparsed);
    }

    public function test_begin_deparsed_with_not_deferrable() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = BeginBuilder::create()
            ->notDeferrable();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('BEGIN NOT DEFERRABLE', $deparsed);
    }

    public function test_begin_deparsed_with_read_only() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = BeginBuilder::create()
            ->readOnly();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('BEGIN READ ONLY', $deparsed);
    }

    public function test_begin_deparsed_with_read_write() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = BeginBuilder::create()
            ->readWrite();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('BEGIN READ WRITE', $deparsed);
    }

    public function test_immutability() : void
    {
        $original = BeginBuilder::create();
        $modified = $original->isolationLevel(IsolationLevel::SERIALIZABLE);

        self::assertNotSame($original, $modified);
    }

    private function deparse(TransactionStmt $stmt) : string
    {
        $parser = new Parser();
        $node = new Node();
        $node->setTransactionStmt($stmt);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
