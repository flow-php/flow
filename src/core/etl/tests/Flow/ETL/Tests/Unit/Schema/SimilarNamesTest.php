<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Schema\SimilarNames;
use Flow\ETL\Tests\FlowTestCase;

final class SimilarNamesTest extends FlowTestCase
{
    public function test_a_candidate_containing_the_searched_name_is_always_close(): void
    {
        static::assertSame(
            ['region_code'],
            (new SimilarNames())->closestTo('region', ['month', 'sales', 'region_code']),
        );
    }

    public function test_an_empty_candidate_list_stays_empty(): void
    {
        static::assertSame([], (new SimilarNames())->closestTo('id', []));
    }

    public function test_nothing_close_enough_suggests_nothing(): void
    {
        static::assertSame([], (new SimilarNames())->closestTo('nope', ['id', 'seller', 'amount']));
    }

    public function test_the_cap_is_configurable(): void
    {
        static::assertSame(['scorea'], (new SimilarNames())->closestTo('score', ['scorea', 'scoreb'], 1));
    }

    public function test_the_closest_candidate_comes_first(): void
    {
        static::assertSame(
            ['score', 'scoreid'],
            (new SimilarNames())->closestTo('scoree', ['scoreid', 'score', 'name', 'id']),
        );
    }

    public function test_the_list_is_capped(): void
    {
        static::assertCount(3, (new SimilarNames())->closestTo('score', ['scorea', 'scoreb', 'scorec', 'scored']));
    }

    public function test_a_typo_finds_its_column(): void
    {
        static::assertSame(['score'], (new SimilarNames())->closestTo('scoree', ['id', 'score', 'name']));
    }
}
