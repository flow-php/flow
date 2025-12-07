<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Transaction;

use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\PgQuery\Protobuf\AST\{Node, RawStmt, VariableSetKind, VariableSetStmt};
use Flow\PgQuery\QueryBuilder\Transaction\{IsolationLevel, SetTransactionBuilder};
use PHPUnit\Framework\TestCase;

final class SetTransactionBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_immutability() : void
    {
        $original = SetTransactionBuilder::create();
        $modified = $original->isolationLevel(IsolationLevel::SERIALIZABLE);

        self::assertNotSame($original, $modified);
    }

    public function test_set_session_transaction_deparsed() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = SetTransactionBuilder::session()
            ->isolationLevel(IsolationLevel::SERIALIZABLE);

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE', $deparsed);
    }

    public function test_set_transaction_ast_has_correct_kind() : void
    {
        $query = SetTransactionBuilder::create()
            ->isolationLevel(IsolationLevel::SERIALIZABLE);

        $ast = $query->toAst();

        self::assertInstanceOf(VariableSetStmt::class, $ast);
        self::assertSame(VariableSetKind::VAR_SET_MULTI, $ast->getKind());
        self::assertSame('TRANSACTION', $ast->getName());
    }

    public function test_set_transaction_deparsed_with_isolation_level() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = SetTransactionBuilder::create()
            ->isolationLevel(IsolationLevel::READ_COMMITTED);

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('SET TRANSACTION ISOLATION LEVEL READ COMMITTED', $deparsed);
    }

    public function test_set_transaction_deparsed_with_multiple_options() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = SetTransactionBuilder::create()
            ->isolationLevel(IsolationLevel::SERIALIZABLE)
            ->readOnly()
            ->deferrable();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE', $deparsed);
    }

    public function test_set_transaction_deparsed_with_read_only() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = SetTransactionBuilder::create()
            ->readOnly();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('SET TRANSACTION READ ONLY', $deparsed);
    }

    public function test_set_transaction_deparsed_with_read_write() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = SetTransactionBuilder::create()
            ->readWrite();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('SET TRANSACTION READ WRITE', $deparsed);
    }

    public function test_set_transaction_snapshot_deparsed() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = SetTransactionBuilder::create()
            ->snapshot('00000003-0000001A-1');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("SET TRANSACTION SNAPSHOT '00000003-0000001A-1'", $deparsed);
    }

    private function deparse(VariableSetStmt $stmt) : string
    {
        $parser = new Parser();
        $node = new Node();
        $node->setVariableSetStmt($stmt);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}
