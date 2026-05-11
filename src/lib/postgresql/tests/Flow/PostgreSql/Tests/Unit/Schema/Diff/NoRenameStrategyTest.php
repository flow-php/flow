<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\NoRenameStrategy;
use Flow\PostgreSql\Schema\Diff\RenameCandidate;
use PHPUnit\Framework\TestCase;

final class NoRenameStrategyTest extends TestCase
{
    public function test_always_returns_empty(): void
    {
        $strategy = new NoRenameStrategy();

        static::assertSame(
            [],
            $strategy->resolve([
                new RenameCandidate('full_name', 'name'),
            ]),
        );
    }

    public function test_empty_candidates_returns_empty(): void
    {
        $strategy = new NoRenameStrategy();

        static::assertSame([], $strategy->resolve([]));
    }

    public function test_multiple_candidates_returns_empty(): void
    {
        $strategy = new NoRenameStrategy();

        static::assertSame(
            [],
            $strategy->resolve([
                new RenameCandidate('col_a', 'col_x'),
                new RenameCandidate('col_b', 'col_y'),
            ]),
        );
    }
}
