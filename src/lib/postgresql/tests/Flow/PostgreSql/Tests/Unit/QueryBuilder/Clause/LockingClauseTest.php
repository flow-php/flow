<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\QueryBuilder\Clause\LockingClause;
use Flow\PostgreSql\QueryBuilder\Clause\LockStrength;
use Flow\PostgreSql\QueryBuilder\Clause\LockWaitPolicy;
use PHPUnit\Framework\TestCase;

final class LockingClauseTest extends TestCase
{
    public function test_for_key_share(): void
    {
        $lock = LockingClause::forKeyShare();

        static::assertSame(LockStrength::KEY_SHARE, $lock->strength());
        static::assertEmpty($lock->tables());
    }

    public function test_for_key_share_with_tables(): void
    {
        $lock = LockingClause::forKeyShare(['users', 'posts']);

        static::assertSame(LockStrength::KEY_SHARE, $lock->strength());
        static::assertCount(2, $lock->tables());
    }

    public function test_for_no_key_update(): void
    {
        $lock = LockingClause::forNoKeyUpdate();

        static::assertSame(LockStrength::NO_KEY_UPDATE, $lock->strength());
    }

    public function test_for_share(): void
    {
        $lock = LockingClause::forShare();

        static::assertSame(LockStrength::SHARE, $lock->strength());
    }

    public function test_for_share_nowait(): void
    {
        $lock = LockingClause::forShare()->nowait();

        static::assertSame(LockWaitPolicy::NOWAIT, $lock->waitPolicy());
    }

    public function test_for_share_skip_locked(): void
    {
        $lock = LockingClause::forShare()->skipLocked();

        static::assertSame(LockWaitPolicy::SKIP_LOCKED, $lock->waitPolicy());
    }

    public function test_for_update(): void
    {
        $lock = LockingClause::forUpdate();

        static::assertSame(LockStrength::UPDATE, $lock->strength());
        static::assertSame(LockWaitPolicy::DEFAULT, $lock->waitPolicy());
    }

    public function test_for_update_skip_locked(): void
    {
        $lock = LockingClause::forUpdate()->skipLocked();

        static::assertSame(LockStrength::UPDATE, $lock->strength());
        static::assertSame(LockWaitPolicy::SKIP_LOCKED, $lock->waitPolicy());
    }

    public function test_for_update_with_tables(): void
    {
        $lock = LockingClause::forUpdate(['users']);

        static::assertSame(LockStrength::UPDATE, $lock->strength());
        static::assertCount(1, $lock->tables());
        static::assertSame(['users'], $lock->tables());
    }

    public function test_to_ast(): void
    {
        $lock = LockingClause::forUpdate(['users']);
        $node = $lock->toAst();
        $lockingClause = $node->getLockingClause();

        static::assertNotNull($lockingClause);
    }

    public function test_to_ast_and_from_ast(): void
    {
        $lock = LockingClause::forUpdate(['users'])->nowait();
        $node = $lock->toAst();
        $restored = LockingClause::fromAst($node);

        static::assertSame($lock->strength(), $restored->strength());
        static::assertSame($lock->waitPolicy(), $restored->waitPolicy());
        static::assertSame($lock->tables(), $restored->tables());
    }
}
