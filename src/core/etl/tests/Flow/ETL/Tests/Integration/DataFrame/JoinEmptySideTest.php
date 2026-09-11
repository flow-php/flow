<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Join\Join;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JoinEmptySideTest extends FlowTestCase
{
    public function test_cross_join_against_an_empty_side_yields_no_rows_under_the_cross_schema(): void
    {
        $joined = df()
            ->read(from_rows(rows(schema(int_schema('id')), row(['id' => 1]))))
            ->crossJoin(df()->process(rows(schema(str_schema('code')))))
            ->fetch();

        static::assertSame([], $joined->toArray());
        static::assertSame(['id', 'code'], $joined->schema()->references()->names());
    }

    /**
     * A single bucket forces Joiner::joinBuildingLeft(), 64 takes the default path.
     */
    public function test_left_join_against_an_empty_right_side_keeps_the_right_columns(): void
    {
        foreach ([$this->leftJoinedAgainstNothing(1), $this->leftJoinedAgainstNothing(64)] as $joined) {
            static::assertSame(['id', 'r'], $joined->schema()->references()->names());
            static::assertSame([['id' => 1, 'r' => null], ['id' => 2, 'r' => null]], $joined->toArray());
        }
    }

    public function test_right_join_against_an_empty_left_side_keeps_the_left_columns(): void
    {
        foreach ([$this->rightJoinedAgainstNothing(1), $this->rightJoinedAgainstNothing(64)] as $joined) {
            // a RIGHT join drops the left side's duplicate join column, so `id` comes from the right
            static::assertSame(['l', 'id'], $joined->schema()->references()->names());
            static::assertSame([['l' => null, 'id' => 1]], $joined->toArray());
        }
    }

    /**
     * @param int<1, max> $buckets
     */
    public function leftJoinedAgainstNothing(int $buckets): Rows
    {
        return df()
            ->read(from_rows(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]))))
            ->join(
                df()->process(rows(schema(int_schema('id'), str_schema('r')))),
                join_on(['id' => 'id']),
                Join::left,
                hash_join()->bucketsCount($buckets),
            )
            ->fetch();
    }

    /**
     * @param int<1, max> $buckets
     */
    public function rightJoinedAgainstNothing(int $buckets): Rows
    {
        return df()
            ->read(from_rows(rows(schema(int_schema('id'), str_schema('l')))))
            ->join(
                df()->process(rows(schema(int_schema('id')), row(['id' => 1]))),
                join_on(['id' => 'id']),
                Join::right,
                hash_join()->bucketsCount($buckets),
            )
            ->fetch();
    }
}
