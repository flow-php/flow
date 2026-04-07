<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\{GreedySimilarityRenameStrategy, LevenshteinStrategy, RenameCandidate, SimilarTextStrategy};
use PHPUnit\Framework\TestCase;

final class GreedySimilarityRenameStrategyTest extends TestCase
{
    public function test_ambiguous_candidates_resolved_by_similarity() : void
    {
        $strategy = new GreedySimilarityRenameStrategy(new SimilarTextStrategy());

        $result = $strategy->resolve([
            new RenameCandidate('net_commission', 'net_commission_cents'),
            new RenameCandidate('net_commission', 'retail_agency_commission_cents'),
            new RenameCandidate('net_commission', 'total_commission_cents'),
            new RenameCandidate('retail_agency_commission', 'net_commission_cents'),
            new RenameCandidate('retail_agency_commission', 'retail_agency_commission_cents'),
            new RenameCandidate('retail_agency_commission', 'total_commission_cents'),
            new RenameCandidate('total_commission', 'net_commission_cents'),
            new RenameCandidate('total_commission', 'retail_agency_commission_cents'),
            new RenameCandidate('total_commission', 'total_commission_cents'),
        ]);

        $renames = [];

        foreach ($result as $match) {
            $renames[$match->removedName] = $match->addedName;
        }

        \ksort($renames);

        self::assertSame([
            'net_commission_cents' => 'net_commission',
            'retail_agency_commission_cents' => 'retail_agency_commission',
            'total_commission_cents' => 'total_commission',
        ], $renames);
    }

    public function test_below_threshold_not_matched() : void
    {
        $strategy = new GreedySimilarityRenameStrategy(new SimilarTextStrategy());

        $result = $strategy->resolve([
            new RenameCandidate('xyz_completely_different', 'foo'),
            new RenameCandidate('xyz_completely_different', 'bar'),
        ]);

        self::assertSame([], $result);
    }

    public function test_different_similarity_strategy_changes_matching_outcome() : void
    {
        $candidates = [
            new RenameCandidate('net_commission', 'net_commission_cents'),
            new RenameCandidate('net_commission', 'total_commission_cents'),
            new RenameCandidate('total_commission', 'net_commission_cents'),
            new RenameCandidate('total_commission', 'total_commission_cents'),
        ];

        $withSimilarText = new GreedySimilarityRenameStrategy(new SimilarTextStrategy());
        $withLevenshtein = new GreedySimilarityRenameStrategy(new LevenshteinStrategy());

        $similarTextResult = $withSimilarText->resolve($candidates);
        $levenshteinResult = $withLevenshtein->resolve($candidates);

        self::assertCount(2, $similarTextResult);
        self::assertCount(2, $levenshteinResult);

        $stRenames = [];

        foreach ($similarTextResult as $m) {
            $stRenames[$m->removedName] = $m->addedName;
        }

        $lvRenames = [];

        foreach ($levenshteinResult as $m) {
            $lvRenames[$m->removedName] = $m->addedName;
        }

        self::assertSame('net_commission', $stRenames['net_commission_cents']);
        self::assertSame('total_commission', $stRenames['total_commission_cents']);
        self::assertSame('net_commission', $lvRenames['net_commission_cents']);
        self::assertSame('total_commission', $lvRenames['total_commission_cents']);
    }

    public function test_empty_candidates_returns_empty() : void
    {
        $strategy = new GreedySimilarityRenameStrategy(new SimilarTextStrategy());

        self::assertSame([], $strategy->resolve([]));
    }

    public function test_greedy_picks_highest_similarity_first() : void
    {
        $strategy = new GreedySimilarityRenameStrategy(new SimilarTextStrategy());

        $result = $strategy->resolve([
            new RenameCandidate('full_name', 'first_name'),
            new RenameCandidate('full_name', 'last_name'),
        ]);

        self::assertCount(1, $result);
        self::assertSame('full_name', $result[0]->addedName);
        self::assertSame('last_name', $result[0]->removedName);
    }

    public function test_higher_threshold_rejects_matches_that_default_accepts() : void
    {
        $candidates = [
            new RenameCandidate('full_name', 'first_name'),
            new RenameCandidate('full_name', 'last_name'),
        ];

        $defaultStrategy = new GreedySimilarityRenameStrategy(new SimilarTextStrategy());
        $strictStrategy = new GreedySimilarityRenameStrategy(new SimilarTextStrategy(90.0));

        self::assertCount(1, $defaultStrategy->resolve($candidates));
        self::assertSame([], $strictStrategy->resolve($candidates));
    }

    public function test_same_removed_not_matched_twice() : void
    {
        $strategy = new GreedySimilarityRenameStrategy(new SimilarTextStrategy());

        $result = $strategy->resolve([
            new RenameCandidate('col_x', 'col_old'),
            new RenameCandidate('col_y', 'col_old'),
        ]);

        self::assertCount(1, $result);
        self::assertSame('col_old', $result[0]->removedName);
    }

    public function test_unambiguous_match_consumed_before_ambiguous_pass() : void
    {
        $strategy = new GreedySimilarityRenameStrategy(new SimilarTextStrategy());

        $result = $strategy->resolve([
            new RenameCandidate('email', 'email_address'),
            new RenameCandidate('name', 'email_address'),
            new RenameCandidate('name', 'full_name'),
        ]);

        $renames = [];

        foreach ($result as $match) {
            $renames[$match->removedName] = $match->addedName;
        }

        self::assertCount(2, $renames);
        self::assertSame('email', $renames['email_address']);
        self::assertSame('name', $renames['full_name']);
    }

    public function test_unambiguous_matched_first_then_ambiguous_resolved() : void
    {
        $strategy = new GreedySimilarityRenameStrategy(new SimilarTextStrategy());

        $result = $strategy->resolve([
            new RenameCandidate('unique_col', 'old_unique_col'),
            new RenameCandidate('amount', 'amount_cents'),
            new RenameCandidate('amount', 'total_cents'),
            new RenameCandidate('total', 'amount_cents'),
            new RenameCandidate('total', 'total_cents'),
        ]);

        $renames = [];

        foreach ($result as $match) {
            $renames[$match->removedName] = $match->addedName;
        }

        self::assertArrayHasKey('old_unique_col', $renames);
        self::assertSame('unique_col', $renames['old_unique_col']);
        self::assertArrayHasKey('amount_cents', $renames);
        self::assertSame('amount', $renames['amount_cents']);
        self::assertArrayHasKey('total_cents', $renames);
        self::assertSame('total', $renames['total_cents']);
    }

    public function test_unambiguous_single_match() : void
    {
        $strategy = new GreedySimilarityRenameStrategy(new SimilarTextStrategy());

        $result = $strategy->resolve([
            new RenameCandidate('full_name', 'name'),
        ]);

        self::assertCount(1, $result);
        self::assertSame('full_name', $result[0]->addedName);
        self::assertSame('name', $result[0]->removedName);
    }
}
