<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\QueryBuilder\Clause\{LockStrength, LockWaitPolicy, LockingClause};
use PHPUnit\Framework\TestCase;

final class LockingClauseTest extends TestCase
{
    public function test_for_key_share() : void
    {
        $lock = LockingClause::forKeyShare();

        self::assertSame(LockStrength::KEY_SHARE, $lock->strength());
        self::assertEmpty($lock->tables());
    }

    public function test_for_key_share_with_tables() : void
    {
        $lock = LockingClause::forKeyShare(['users', 'posts']);

        self::assertSame(LockStrength::KEY_SHARE, $lock->strength());
        self::assertCount(2, $lock->tables());
    }

    public function test_for_no_key_update() : void
    {
        $lock = LockingClause::forNoKeyUpdate();

        self::assertSame(LockStrength::NO_KEY_UPDATE, $lock->strength());
    }

    public function test_for_share() : void
    {
        $lock = LockingClause::forShare();

        self::assertSame(LockStrength::SHARE, $lock->strength());
    }

    public function test_for_share_nowait() : void
    {
        $lock = LockingClause::forShare()->nowait();

        self::assertSame(LockWaitPolicy::NOWAIT, $lock->waitPolicy());
    }

    public function test_for_share_skip_locked() : void
    {
        $lock = LockingClause::forShare()->skipLocked();

        self::assertSame(LockWaitPolicy::SKIP_LOCKED, $lock->waitPolicy());
    }

    public function test_for_update() : void
    {
        $lock = LockingClause::forUpdate();

        self::assertSame(LockStrength::UPDATE, $lock->strength());
        self::assertSame(LockWaitPolicy::DEFAULT, $lock->waitPolicy());
    }

    public function test_for_update_with_tables() : void
    {
        $lock = LockingClause::forUpdate(['users']);

        self::assertSame(LockStrength::UPDATE, $lock->strength());
        self::assertCount(1, $lock->tables());
        self::assertSame(['users'], $lock->tables());
    }

    public function test_to_ast() : void
    {
        $lock = LockingClause::forUpdate(['users']);
        $node = $lock->toAst();
        $lockingClause = $node->getLockingClause();

        self::assertNotNull($lockingClause);
    }

    public function test_to_ast_and_from_ast() : void
    {
        $lock = LockingClause::forUpdate(['users'])->nowait();
        $node = $lock->toAst();
        $restored = LockingClause::fromAst($node);

        self::assertSame($lock->strength(), $restored->strength());
        self::assertSame($lock->waitPolicy(), $restored->waitPolicy());
        self::assertSame($lock->tables(), $restored->tables());
    }
}
