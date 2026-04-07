<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\{RenameCandidate, StrictRenameStrategy};
use PHPUnit\Framework\TestCase;

final class StrictRenameStrategyTest extends TestCase
{
    public function test_ambiguous_candidates_not_matched() : void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('net_commission', 'net_commission_cents'),
            new RenameCandidate('net_commission', 'total_commission_cents'),
        ]);

        self::assertSame([], $result);
    }

    public function test_empty_candidates_returns_empty() : void
    {
        $strategy = new StrictRenameStrategy();

        self::assertSame([], $strategy->resolve([]));
    }

    public function test_mixed_ambiguous_and_unambiguous() : void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('unique_col', 'old_unique'),
            new RenameCandidate('ambig_a', 'old_x'),
            new RenameCandidate('ambig_a', 'old_y'),
        ]);

        self::assertCount(1, $result);
        self::assertSame('unique_col', $result[0]->addedName);
        self::assertSame('old_unique', $result[0]->removedName);
    }

    public function test_multiple_unambiguous_matches() : void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('col_a', 'col_x'),
            new RenameCandidate('col_b', 'col_y'),
        ]);

        self::assertCount(2, $result);
    }

    public function test_reverse_ambiguity_two_added_one_removed_candidate() : void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('col_new_a', 'col_old'),
            new RenameCandidate('col_new_b', 'col_old'),
        ]);

        self::assertCount(1, $result);
        self::assertSame('col_new_a', $result[0]->addedName);
        self::assertSame('col_old', $result[0]->removedName);
    }

    public function test_same_removed_matched_only_once() : void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('col_a', 'col_shared'),
            new RenameCandidate('col_b', 'col_shared'),
        ]);

        self::assertCount(1, $result);
        self::assertSame('col_a', $result[0]->addedName);
        self::assertSame('col_shared', $result[0]->removedName);
    }

    public function test_three_way_ambiguity_no_matches() : void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('a', 'x'),
            new RenameCandidate('a', 'y'),
            new RenameCandidate('b', 'x'),
            new RenameCandidate('b', 'y'),
        ]);

        self::assertSame([], $result);
    }

    public function test_unambiguous_single_match() : void
    {
        $strategy = new StrictRenameStrategy();

        $result = $strategy->resolve([
            new RenameCandidate('full_name', 'name'),
        ]);

        self::assertCount(1, $result);
        self::assertSame('full_name', $result[0]->addedName);
        self::assertSame('name', $result[0]->removedName);
    }
}
