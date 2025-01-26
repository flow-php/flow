<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\{bool_entry, int_entry, join_on, row, rows, str_entry};
use Flow\ETL\Exception\{DuplicatedEntriesException, InvalidArgumentException};
use Flow\ETL\Join\Expression;
use Flow\ETL\Tests\FlowTestCase;

final class RowsJoinTest extends FlowTestCase
{
    public function test_cross_join() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'PL')),
            row(int_entry('id', 3), str_entry('country', 'US')),
            row(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $joined = $left->joinCross(
            rows(
                row(int_entry('num', 1), bool_entry('active', true)),
                row(int_entry('num', 2), bool_entry('active', false)),
            ),
        );

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_num' => 1, 'joined_active' => true],
                ['id' => 1, 'country' => 'PL', 'joined_num' => 2, 'joined_active' => false],
                ['id' => 2, 'country' => 'PL', 'joined_num' => 1, 'joined_active' => true],
                ['id' => 2, 'country' => 'PL', 'joined_num' => 2, 'joined_active' => false],
                ['id' => 3, 'country' => 'US', 'joined_num' => 1, 'joined_active' => true],
                ['id' => 3, 'country' => 'US', 'joined_num' => 2, 'joined_active' => false],
                ['id' => 4, 'country' => 'FR', 'joined_num' => 1, 'joined_active' => true],
                ['id' => 4, 'country' => 'FR', 'joined_num' => 2, 'joined_active' => false],

            ],
            $joined->toArray(),
        );
    }

    public function test_cross_join_empty() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'PL')),
            row(int_entry('id', 3), str_entry('country', 'US')),
            row(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $joined = $left->joinCross(
            rows(),
        );

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            $joined->toArray()
        );
    }

    public function test_cross_join_left_empty() : void
    {
        $left = rows();

        $joined = $left->joinCross(
            rows(
                row(int_entry('id', 1), str_entry('country', 'PL')),
                row(int_entry('id', 2), str_entry('country', 'PL')),
                row(int_entry('id', 3), str_entry('country', 'US')),
                row(int_entry('id', 4), str_entry('country', 'FR')),
            ),
        );

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            $joined->toArray()
        );
    }

    public function test_cross_join_left_with_name_conflict() : void
    {
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id, country, active] + [active]. Please consider using join prefix option');

        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL'), bool_entry('active', false)),
            row(int_entry('id', 2), str_entry('country', 'PL'), bool_entry('active', false)),
            row(int_entry('id', 3), str_entry('country', 'US'), bool_entry('active', false)),
            row(int_entry('id', 4), str_entry('country', 'FR'), bool_entry('active', false)),
        );

        $joined = $left->joinCross(
            rows(
                row(bool_entry('active', true))
            ),
            ''
        );
    }

    public function test_cross_join_left_with_name_conflict_with_prefix() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL'), bool_entry('active', false)),
            row(int_entry('id', 2), str_entry('country', 'PL'), bool_entry('active', false)),
            row(int_entry('id', 3), str_entry('country', 'US'), bool_entry('active', false)),
            row(int_entry('id', 4), str_entry('country', 'FR'), bool_entry('active', false)),
        );

        $joined = $left->joinCross(
            rows(
                row(bool_entry('active', true))
            ),
            '_'
        );

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'active' => false, '_active' => true],
                ['id' => 2, 'country' => 'PL', 'active' => false, '_active' => true],
                ['id' => 3, 'country' => 'US', 'active' => false, '_active' => true],
                ['id' => 4, 'country' => 'FR', 'active' => false, '_active' => true],
            ],
            $joined->toArray()
        );
    }

    public function test_inner_empty() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'PL')),
            row(int_entry('id', 3), str_entry('country', 'US')),
            row(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $joined = $left->joinInner(
            rows(),
            Expression::on(['country' => 'code'])
        );

        self::assertEquals(
            rows(),
            $joined
        );
    }

    public function test_inner_join() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'PL')),
            row(int_entry('id', 3), str_entry('country', 'US')),
            row(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $joined = $left->joinInner(
            rows(
                row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                row(str_entry('code', 'US'), str_entry('name', 'United States')),
                row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            join_on(['country' => 'code'], 'joined_')
        );

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 3, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
            ],
            $joined->toArray()
        );
    }

    public function test_inner_join_into_empty() : void
    {
        $left = rows();

        $joined = $left->joinInner(
            rows(
                row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                row(str_entry('code', 'US'), str_entry('name', 'United States')),
                row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        self::assertEquals(
            rows(),
            $joined
        );
    }

    public function test_inner_join_with_duplicated_entries() : void
    {
        $this->expectException(DuplicatedEntriesException::class);
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id, country] + [id, code, name] try to use a different join prefix than: ""');

        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'PL')),
            row(int_entry('id', 3), str_entry('country', 'US')),
            row(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $left->joinInner(
            rows(
                row(int_entry('id', 101), str_entry('code', 'PL'), str_entry('name', 'Poland')),
                row(int_entry('id', 102), str_entry('code', 'US'), str_entry('name', 'United States')),
                row(int_entry('id', 103), str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'], joinPrefix: '')
        );
    }

    public function test_inner_join_without_prefix() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country_code', 'PL')),
            row(int_entry('id', 2), str_entry('country_code', 'PL')),
            row(int_entry('id', 3), str_entry('country_code', 'US')),
            row(int_entry('id', 4), str_entry('country_code', 'FR')),
        );

        $joined = $left->joinInner(
            rows(
                row(str_entry('country_code', 'PL'), str_entry('name', 'Poland')),
                row(str_entry('country_code', 'US'), str_entry('name', 'United States')),
                row(str_entry('country_code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            join_on(['country_code' => 'country_code'])
        );

        self::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 3, 'country_code' => 'US', 'name' => 'United States'],
            ],
            $joined->toArray()
        );
    }

    public function test_left_anti_join() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'US')),
            row(int_entry('id', 3), str_entry('country', 'FR')),
        );

        $joined = $left->joinLeftAnti(
            rows(
                row(str_entry('code', 'US'), str_entry('name', 'United States')),
                row(str_entry('code', 'FR'), str_entry('name', 'France')),
            ),
            Expression::on(['country' => 'code'])
        );

        self::assertEquals(
            rows(
                row(int_entry('id', 1), str_entry('country', 'PL')),
            ),
            $joined
        );
    }

    public function test_left_anti_join_on_empty() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'US')),
            row(int_entry('id', 3), str_entry('country', 'FR')),
        );

        $joined = $left->joinLeftAnti(
            rows(),
            Expression::on(['country' => 'code'])
        );

        self::assertEquals(
            $left,
            $joined
        );
    }

    public function test_left_anti_join_without_prefix() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country_code', 'PL')),
            row(int_entry('id', 2), str_entry('country_code', 'US')),
            row(int_entry('id', 3), str_entry('country_code', 'FR')),
        );

        $joined = $left->joinLeftAnti(
            rows(
                row(str_entry('country_code', 'US'), str_entry('name', 'United States')),
                row(str_entry('country_code', 'FR'), str_entry('name', 'France')),
            ),
            Expression::on(['country_code' => 'country_code'])
        );

        self::assertEquals(
            rows(
                row(int_entry('id', 1), str_entry('country_code', 'PL')),
            ),
            $joined
        );
    }

    public function test_left_join() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'US')),
            row(int_entry('id', 3), str_entry('country', 'FR')),
        );

        $joined = $left->joinLeft(
            rows(
                row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                row(str_entry('code', 'US'), str_entry('name', 'United States')),
                row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'], 'joined_')
        );

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 3, 'country' => 'FR', 'joined_code' => null, 'joined_name' => null],
            ],
            $joined->toArray()
        );
    }

    public function test_left_join_empty() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'US')),
            row(int_entry('id', 3), str_entry('country', 'FR')),
        );

        $joined = $left->joinLeft(
            rows(),
            Expression::on(['country' => 'code'])
        );

        self::assertEquals(
            rows(
                row(int_entry('id', 1), str_entry('country', 'PL')),
                row(int_entry('id', 2), str_entry('country', 'US')),
                row(int_entry('id', 3), str_entry('country', 'FR')),
            ),
            $joined
        );
    }

    public function test_left_join_empty_without_prefix() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country_code', 'PL')),
            row(int_entry('id', 2), str_entry('country_code', 'US')),
            row(int_entry('id', 3), str_entry('country_code', 'FR')),
        );

        $joined = $left->joinLeft(
            rows(),
            Expression::on(['country_code' => 'country_code'])
        );

        self::assertEquals(
            rows(
                row(int_entry('id', 1), str_entry('country_code', 'PL')),
                row(int_entry('id', 2), str_entry('country_code', 'US')),
                row(int_entry('id', 3), str_entry('country_code', 'FR')),
            ),
            $joined
        );
    }

    public function test_left_join_to_empty() : void
    {
        $left = rows();

        $joined = $left->joinLeft(
            rows(
                row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                row(str_entry('code', 'US'), str_entry('name', 'United States')),
                row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'])
        );

        self::assertEquals(
            rows(),
            $joined
        );
    }

    public function test_left_join_with_the_duplicated_columns() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Merged entries names must be unique, given: [id, country] + [id, code, name] try to use a different join prefix than: ""');

        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'US')),
            row(int_entry('id', 3), str_entry('country', 'FR')),
        );

        $left->joinLeft(
            rows(
                row(int_entry('id', 100), str_entry('code', 'PL'), str_entry('name', 'Poland')),
                row(int_entry('id', 101), str_entry('code', 'US'), str_entry('name', 'United States')),
                row(int_entry('id', 102), str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'], '')
        );
    }

    public function test_left_join_without_prefix() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country_code', 'PL')),
            row(int_entry('id', 2), str_entry('country_code', 'US')),
            row(int_entry('id', 3), str_entry('country_code', 'FR')),
        );

        $joined = $left->joinLeft(
            rows(
                row(str_entry('country_code', 'PL'), str_entry('name', 'Poland')),
                row(str_entry('country_code', 'US'), str_entry('name', 'United States')),
                row(str_entry('country_code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country_code' => 'country_code'])
        );

        self::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => 3, 'country_code' => 'FR', 'name' => null],
            ],
            $joined->toArray()
        );
    }

    public function test_right_join() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'PL')),
            row(int_entry('id', 3), str_entry('country', 'US')),
            row(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $joined = $left->joinRight(
            rows(
                row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                row(str_entry('code', 'US'), str_entry('name', 'United States')),
                row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'], 'joined_')
        );

        self::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 3, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => null, 'country' => null, 'joined_code' => 'GB', 'joined_name' => 'Great Britain'],
            ],
            $joined->toArray()
        );
    }

    public function test_right_join_empty() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'PL')),
            row(int_entry('id', 3), str_entry('country', 'US')),
            row(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $joined = $left->joinRight(
            rows(),
            Expression::on(['country' => 'code'])
        );

        self::assertEquals(
            rows(),
            $joined
        );
    }

    public function test_right_join_to_empty() : void
    {
        $left = rows();

        $joined = $left->joinRight(
            rows(
                row(str_entry('code', 'PL'), str_entry('name', 'Poland')),
                row(str_entry('code', 'US'), str_entry('name', 'United States')),
                row(str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'], 'joined_')
        );

        self::assertEquals(
            [
                ['joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['joined_code' => 'US', 'joined_name' => 'United States'],
                ['joined_code' => 'GB', 'joined_name' => 'Great Britain'],
            ],
            $joined->toArray()
        );
    }

    public function test_right_join_with_duplicated_entry_names() : void
    {
        $this->expectException(DuplicatedEntriesException::class);
        $this->expectExceptionMessage('erged entries names must be unique, given: [id, country] + [id, code, name] try to use a different join prefix than: ""');

        $left = rows(
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'PL')),
            row(int_entry('id', 3), str_entry('country', 'US')),
            row(int_entry('id', 4), str_entry('country', 'FR')),
        );

        $left->joinRight(
            rows(
                row(int_entry('id', 101), str_entry('code', 'PL'), str_entry('name', 'Poland')),
                row(int_entry('id', 102), str_entry('code', 'US'), str_entry('name', 'United States')),
                row(int_entry('id', 103), str_entry('code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country' => 'code'], '')
        );
    }

    public function test_right_join_without_prefix() : void
    {
        $left = rows(
            row(int_entry('id', 1), str_entry('country_code', 'PL')),
            row(int_entry('id', 2), str_entry('country_code', 'PL')),
            row(int_entry('id', 3), str_entry('country_code', 'US')),
            row(int_entry('id', 4), str_entry('country_code', 'FR')),
        );

        $joined = $left->joinRight(
            rows(
                row(str_entry('country_code', 'PL'), str_entry('name', 'Poland')),
                row(str_entry('country_code', 'US'), str_entry('name', 'United States')),
                row(str_entry('country_code', 'GB'), str_entry('name', 'Great Britain')),
            ),
            Expression::on(['country_code' => 'country_code'])
        );

        self::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 3, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => null, 'country_code' => 'GB', 'name' => 'Great Britain'],
            ],
            $joined->toArray()
        );
    }
}
