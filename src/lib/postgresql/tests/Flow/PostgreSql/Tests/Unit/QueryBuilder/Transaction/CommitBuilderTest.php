<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Transaction;

use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RawStmt;
use Flow\PostgreSql\Protobuf\AST\TransactionStmt;
use Flow\PostgreSql\Protobuf\AST\TransactionStmtKind;
use Flow\PostgreSql\QueryBuilder\Transaction\CommitBuilder;
use PHPUnit\Framework\TestCase;

final class CommitBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_commit_ast_has_correct_kind(): void
    {
        $query = CommitBuilder::create();

        $ast = $query->toAst();

        static::assertInstanceOf(TransactionStmt::class, $ast);
        static::assertSame(TransactionStmtKind::TRANS_STMT_COMMIT, $ast->getKind());
        static::assertFalse($ast->getChain());
    }

    public function test_commit_deparsed_basic(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CommitBuilder::create();

        $deparsed = $this->deparse($query->toAst());
        static::assertSame('COMMIT', $deparsed);
    }

    public function test_commit_deparsed_with_and_chain(): void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = CommitBuilder::create()->andChain();

        $deparsed = $this->deparse($query->toAst());
        static::assertSame('COMMIT AND CHAIN', $deparsed);
    }

    public function test_commit_with_and_no_chain_has_chain_false(): void
    {
        $query = CommitBuilder::create()->andNoChain();

        $ast = $query->toAst();

        static::assertFalse($ast->getChain());
    }

    public function test_immutability(): void
    {
        $original = CommitBuilder::create();
        $modified = $original->andChain();

        static::assertNotSame($original, $modified);
    }

    private function deparse(TransactionStmt $stmt): string
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
