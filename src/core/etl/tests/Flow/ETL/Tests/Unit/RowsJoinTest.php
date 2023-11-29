<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\str_entry;
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
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'PL')),
            Row::create(int_entry('id', 3), str_entry('country', 'US')),
            Row::create(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $joined = $left->joinCross(
            new Rows(
                Row::create(int_entry('num', 1), bool_entry('active', true)),
                Row::create(int_entry('num', 2), bool_entry('active', false)),
            ),
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('num', 1), bool_entry('active', true)),
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('num', 2), bool_entry('active', false)),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('num', 1), bool_entry('active', true)),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('num', 2), bool_entry('active', false)),
                Row::create(int_entry('id', 3), str_entry('country', 'US'), int_entry('num', 1), bool_entry('active', true)),
                Row::create(int_entry('id', 3), str_entry('country', 'US'), int_entry('num', 2), bool_entry('active', false)),
                Row::create(int_entry('id', 4), str_entry('country', 'FR'), int_entry('num', 1), bool_entry('active', true)),
                Row::create(int_entry('id', 4), str_entry('country', 'FR'), int_entry('num', 2), bool_entry('active', false)),
            ),
            $joined,
        );
    }

    public function test_cross_join_empty() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'PL')),
            Row::create(int_entry('id', 3), str_entry('country', 'US')),
            Row::create(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $joined = $left->joinCross(
            new Rows(),
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL')),
                Row::create(int_entry('id', 2), str_entry('country', 'PL')),
                Row::create(int_entry('id', 3), str_entry('country', 'US')),
                Row::create(int_entry('id', 4), str_entry('country', 'FR')),
            ),
            $joined
        );
    }

    public function test_cross_join_left_empty() : void
    {
        $left = new Rows();

        $joined = $left->joinCross(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL')),
                Row::create(int_entry('id', 2), str_entry('country', 'PL')),
                Row::create(int_entry('id', 3), str_entry('country', 'US')),
                Row::create(int_entry('id', 4), str_entry('country', 'FR')),
            ),
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL')),
                Row::create(int_entry('id', 2), str_entry('country', 'PL')),
                Row::create(int_entry('id', 3), str_entry('country', 'US')),
                Row::create(int_entry('id', 4), str_entry('country', 'FR')),
            ),
            $joined
        );
    }

    public function test_cross_join_left_with_name_conflict() : void
    {
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id, country, active] + [active]. Please consider using join prefix option');

        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL'), bool_entry('active', false)),
            Row::create(int_entry('id', 2), str_entry('country', 'PL'), bool_entry('active', false)),
            Row::create(int_entry('id', 3), str_entry('country', 'US'), bool_entry('active', false)),
            Row::create(int_entry('id', 4), str_entry('country', 'FR'), bool_entry('active', false)),
        );

        $joined = $left->joinCross(
            new Rows(
                Row::create(bool_entry('active', true))
            ),
        );
    }

    public function test_cross_join_left_with_name_conflict_with_prefix() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL'), bool_entry('active', false)),
            Row::create(int_entry('id', 2), str_entry('country', 'PL'), bool_entry('active', false)),
            Row::create(int_entry('id', 3), str_entry('country', 'US'), bool_entry('active', false)),
            Row::create(int_entry('id', 4), str_entry('country', 'FR'), bool_entry('active', false)),
        );

        $joined = $left->joinCross(
            new Rows(
                Row::create(bool_entry('active', true))
            ),
            '_'
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), bool_entry('active', false), bool_entry('_active', true)),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), bool_entry('active', false), bool_entry('_active', true)),
                Row::create(int_entry('id', 3), str_entry('country', 'US'), bool_entry('active', false), bool_entry('_active', true)),
                Row::create(int_entry('id', 4), str_entry('country', 'FR'), bool_entry('active', false), bool_entry('_active', true)),
            ),
            $joined
        );
    }

    public function test_inner_empty() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'PL')),
            Row::create(int_entry('id', 3), str_entry('country', 'US')),
            Row::create(int_entry('id', 4), str_entry('country', 'FR')),
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
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'PL')),
            Row::create(int_entry('id', 3), str_entry('country', 'US')),
            Row::create(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $joined = $left->joinInner(
            new Rows(
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 3), str_entry('country', 'US'), str_entry('name', 'United States')),
            ),
            $joined
        );
    }

    public function test_inner_join_into_empty() : void
    {
        $left = new Rows();

        $joined = $left->joinInner(
            new Rows(
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
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
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'PL')),
            Row::create(int_entry('id', 3), str_entry('country', 'US')),
            Row::create(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $left->joinInner(
            new Rows(
                Row::create(int_entry('id', 101), str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 102), str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(int_entry('id', 103), str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );
    }

    public function test_left_anti_join() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'US')),
            Row::create(int_entry('id', 3), str_entry('country', 'FR')),
        );

        $joined = $left->joinLeftAnti(
            new Rows(
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'FR'), str_entry('name', 'France')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            ),
            $joined
        );
    }

    public function test_left_anti_join_on_empty() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'US')),
            Row::create(int_entry('id', 3), str_entry('country', 'FR')),
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
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'US')),
            Row::create(int_entry('id', 3), str_entry('country', 'FR')),
        );

        $joined = $left->joinLeft(
            new Rows(
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 2), str_entry('country', 'US'), str_entry('name', 'United States')),
                Row::create(int_entry('id', 3), str_entry('country', 'FR'), null_entry('name')),
            ),
            $joined
        );
    }

    public function test_left_join_empty() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'US')),
            Row::create(int_entry('id', 3), str_entry('country', 'FR')),
        );

        $joined = $left->joinLeft(
            new Rows(),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL')),
                Row::create(int_entry('id', 2), str_entry('country', 'US')),
                Row::create(int_entry('id', 3), str_entry('country', 'FR')),
            ),
            $joined
        );
    }

    public function test_left_join_to_empty() : void
    {
        $left = new Rows();

        $joined = $left->joinLeft(
            new Rows(
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
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
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'US')),
            Row::create(int_entry('id', 3), str_entry('country', 'FR')),
        );

        $left->joinLeft(
            new Rows(
                Row::create(int_entry('id', 100), str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 101), str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(int_entry('id', 102), str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );
    }

    public function test_right_join() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'PL')),
            Row::create(int_entry('id', 3), str_entry('country', 'US')),
            Row::create(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $joined = $left->joinRight(
            new Rows(
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland'), int_entry('id', 1)),
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland'), int_entry('id', 2)),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States'), int_entry('id', 3)),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain'), null_entry('id')),
            ),
            $joined
        );
    }

    public function test_right_join_empty() : void
    {
        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'PL')),
            Row::create(int_entry('id', 3), str_entry('country', 'US')),
            Row::create(int_entry('id', 4), str_entry('country', 'FR')),
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
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            $joined
        );
    }

    public function test_right_join_with_duplicated_entry_names() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id, code, name] + [id, country]. Please consider using Condition, join prefix option');

        $left = new Rows(
            Row::create(int_entry('id', 1), str_entry('country', 'PL')),
            Row::create(int_entry('id', 2), str_entry('country', 'PL')),
            Row::create(int_entry('id', 3), str_entry('country', 'US')),
            Row::create(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $left->joinRight(
            new Rows(
                Row::create(int_entry('id', 101), str_entry('code', 'PL'), str_entry('name', 'Poland')),
                Row::create(int_entry('id', 102), str_entry('code', 'US'), str_entry('name', 'United States')),
                Row::create(int_entry('id', 103), str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );
    }
}
