<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\RenameCandidate;
use Flow\PostgreSql\Schema\Diff\StrictRenameStrategy;
use PHPUnit\Framework\TestCase;

final class StrictRenameStrategyTest extends TestCase
{
    public function test_ambiguous_candidates_not_matched(): void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('net_commission', 'net_commission_cents'),
            new RenameCandidate('net_commission', 'total_commission_cents'),
        ]);

        static::assertSame([], $result);
    }

    public function test_empty_candidates_returns_empty(): void
    {
        $strategy = new StrictRenameStrategy();

        static::assertSame([], $strategy->resolve([]));
    }

    public function test_mixed_ambiguous_and_unambiguous(): void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('unique_col', 'old_unique'),
            new RenameCandidate('ambig_a', 'old_x'),
            new RenameCandidate('ambig_a', 'old_y'),
        ]);

        static::assertCount(1, $result);
        static::assertSame('unique_col', $result[0]->addedName);
        static::assertSame('old_unique', $result[0]->removedName);
    }

    public function test_multiple_unambiguous_matches(): void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('col_a', 'col_x'),
            new RenameCandidate('col_b', 'col_y'),
        ]);

        static::assertCount(2, $result);
    }

    public function test_reverse_ambiguity_two_added_one_removed_candidate(): void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('col_new_a', 'col_old'),
            new RenameCandidate('col_new_b', 'col_old'),
        ]);

        static::assertCount(1, $result);
        static::assertSame('col_new_a', $result[0]->addedName);
        static::assertSame('col_old', $result[0]->removedName);
    }

    public function test_same_removed_matched_only_once(): void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('col_a', 'col_shared'),
            new RenameCandidate('col_b', 'col_shared'),
        ]);

        static::assertCount(1, $result);
        static::assertSame('col_a', $result[0]->addedName);
        static::assertSame('col_shared', $result[0]->removedName);
    }

    public function test_three_way_ambiguity_no_matches(): void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('a', 'x'),
            new RenameCandidate('a', 'y'),
            new RenameCandidate('b', 'x'),
            new RenameCandidate('b', 'y'),
        ]);

        static::assertSame([], $result);
    }

    public function test_unambiguous_single_match(): void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('full_name', 'name'),
        ]);

        static::assertCount(1, $result);
        static::assertSame('full_name', $result[0]->addedName);
        static::assertSame('name', $result[0]->removedName);
    }
}
