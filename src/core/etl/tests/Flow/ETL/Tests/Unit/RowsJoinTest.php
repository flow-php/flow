<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Join\Expression;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class RowsJoinTest extends TestCase
{
    public function test_cross_join() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
        );

        $joined = $left->joinCross(
            new Rows(
                Row::create(Entry::integer('num', 1), Entry::boolean('active', true)),
                Row::create(Entry::integer('num', 2), Entry::boolean('active', false)),
            ),
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('num', 1), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('num', 2), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('num', 1), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('num', 2), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'US'), Entry::integer('num', 1), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'US'), Entry::integer('num', 2), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'FR'), Entry::integer('num', 1), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'FR'), Entry::integer('num', 2), Entry::boolean('active', false)),
            ),
            $joined,
        );
    }

    public function test_cross_join_empty() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
        );

        $joined = $left->joinCross(
            new Rows(),
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
            ),
            $joined
        );
    }

    public function test_cross_join_left_empty() : void
    {
        $left = new Rows();

        $joined = $left->joinCross(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
            ),
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
            ),
            $joined
        );
    }

    public function test_cross_join_left_with_name_conflict() : void
    {
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id, country, active] + [active]. Please consider using join prefix option');

        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::boolean('active', false)),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::boolean('active', false)),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'US'), Entry::boolean('active', false)),
            Row::create(Entry::integer('id', 4), Entry::string('country', 'FR'), Entry::boolean('active', false)),
        );

        $joined = $left->joinCross(
            new Rows(
                Row::create(Entry::boolean('active', true))
            ),
        );
    }

    public function test_cross_join_left_with_name_conflict_with_prefix() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::boolean('active', false)),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::boolean('active', false)),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'US'), Entry::boolean('active', false)),
            Row::create(Entry::integer('id', 4), Entry::string('country', 'FR'), Entry::boolean('active', false)),
        );

        $joined = $left->joinCross(
            new Rows(
                Row::create(Entry::boolean('active', true))
            ),
            '_'
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::boolean('active', false), Entry::boolean('_active', true)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::boolean('active', false), Entry::boolean('_active', true)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'US'), Entry::boolean('active', false), Entry::boolean('_active', true)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'FR'), Entry::boolean('active', false), Entry::boolean('_active', true)),
            ),
            $joined
        );
    }

    public function test_inner_empty() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
        );

        $joined = $left->joinInner(
            new Rows(),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(),
            $joined
        );
    }

    public function test_inner_join() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
        );

        $joined = $left->joinInner(
            new Rows(
                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'US'), Entry::string('name', 'United States')),
            ),
            $joined
        );
    }

    public function test_inner_join_into_empty() : void
    {
        $left = new Rows();

        $joined = $left->joinInner(
            new Rows(
                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(),
            $joined
        );
    }

    public function test_inner_join_with_duplicated_entries() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id, country] + [id, code, name]. Please consider using Condition, join prefix option');

        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
        );

        $left->joinInner(
            new Rows(
                Row::create(Entry::integer('id', 101), Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 102), Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 103), Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );
    }

    public function test_left_anti_join() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'FR')),
        );

        $joined = $left->joinLeftAnti(
            new Rows(
                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::string('code', 'FR'), Entry::string('name', 'France')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            ),
            $joined
        );
    }

    public function test_left_anti_join_on_empty() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'FR')),
        );

        $joined = $left->joinLeftAnti(
            new Rows(),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            $left,
            $joined
        );
    }

    public function test_left_join() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'FR')),
        );

        $joined = $left->joinLeft(
            new Rows(
                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'FR'), Entry::null('name')),
            ),
            $joined
        );
    }

    public function test_left_join_empty() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'FR')),
        );

        $joined = $left->joinLeft(
            new Rows(),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'FR')),
            ),
            $joined
        );
    }

    public function test_left_join_to_empty() : void
    {
        $left = new Rows();

        $joined = $left->joinLeft(
            new Rows(
                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(),
            $joined
        );
    }

    public function test_left_join_with_the_duplicated_columns() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id, country] + [id, code, name]. Please consider using Condition, join prefix option');

        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'FR')),
        );

        $left->joinLeft(
            new Rows(
                Row::create(Entry::integer('id', 100), Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 101), Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 102), Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );
    }

    public function test_right_join() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
        );

        $joined = $left->joinRight(
            new Rows(
                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland'), Entry::integer('id', 1)),
                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland'), Entry::integer('id', 2)),
                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States'), Entry::integer('id', 3)),
                Row::create(Entry::string('code', 'GB'), Entry::string('name', 'Great Britain'), Entry::null('id')),
            ),
            $joined
        );
    }

    public function test_right_join_empty() : void
    {
        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
        );

        $joined = $left->joinRight(
            new Rows(),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(),
            $joined
        );
    }

    public function test_right_join_to_empty() : void
    {
        $left = new Rows();

        $joined = $left->joinRight(
            new Rows(
                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
            ),
            $joined
        );
    }

    public function test_right_join_with_duplicated_entry_names() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id, code, name] + [id, country]. Please consider using Condition, join prefix option');

        $left = new Rows(
            Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
            Row::create(Entry::integer('id', 3), Entry::string('country', 'US')),
            Row::create(Entry::integer('id', 4), Entry::string('country', 'FR')),
        );

        $left->joinRight(
            new Rows(
                Row::create(Entry::integer('id', 101), Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 102), Entry::string('code', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 103), Entry::string('code', 'GB'), Entry::string('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );
    }
}
