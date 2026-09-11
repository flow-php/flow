<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Join\Expression;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowsJoinTest extends FlowTestCase
{
    public function test_cross_join(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'PL']),
            row(['id' => 3, 'country' => 'US']),
            row(['id' => 4, 'country' => 'FR']),
        );

        $joined = $left->joinCross(rows(
            schema(int_schema('num'), bool_schema('active')),
            row(['num' => 1, 'active' => true]),
            row(['num' => 2, 'active' => false]),
        ));

        static::assertEquals(
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

    public function test_cross_join_empty(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'PL']),
            row(['id' => 3, 'country' => 'US']),
            row(['id' => 4, 'country' => 'FR']),
        );

        // n x 0 = 0, and a zero-row batch fits the cross schema vacuously
        static::assertSame([], $left->joinCross(rows(schema()))->toArray());
    }

    public function test_cross_join_with_an_empty_side_yields_no_rows_under_the_cross_schema(): void
    {
        // a cross join with an empty side produces zero rows, and the batch still declares both
        // sides' columns - the schema describes the plan, not the cardinality
        $emptyRight = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
        )->joinCross(rows(schema(str_schema('code'))));

        static::assertSame(['id', 'country', 'joined_code'], $emptyRight->schema()->references()->names());
        static::assertSame([], $emptyRight->toArray());

        $emptyLeft = rows(schema(int_schema('id')))->joinCross(rows(schema(str_schema('code')), row(['code' => 'PL'])));

        static::assertSame(['id', 'joined_code'], $emptyLeft->schema()->references()->names());
        static::assertSame([], $emptyLeft->toArray());
    }

    public function test_cross_join_left_empty(): void
    {
        $left = rows(schema());

        $joined = $left->joinCross(rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'PL']),
            row(['id' => 3, 'country' => 'US']),
            row(['id' => 4, 'country' => 'FR']),
        ));

        static::assertSame([], $joined->toArray());
    }

    public function test_cross_join_left_with_name_conflict(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage(
            'Entry definitions must be unique, duplicated entries: [active], all: [id, country, active, active]',
        );

        $left = rows(
            schema(int_schema('id'), str_schema('country'), bool_schema('active')),
            row(['id' => 1, 'country' => 'PL', 'active' => false]),
            row(['id' => 2, 'country' => 'PL', 'active' => false]),
            row(['id' => 3, 'country' => 'US', 'active' => false]),
            row(['id' => 4, 'country' => 'FR', 'active' => false]),
        );

        $left->joinCross(rows(schema(bool_schema('active')), row(['active' => true])), '');
    }

    public function test_cross_join_left_with_name_conflict_with_prefix(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country'), bool_schema('active')),
            row(['id' => 1, 'country' => 'PL', 'active' => false]),
            row(['id' => 2, 'country' => 'PL', 'active' => false]),
            row(['id' => 3, 'country' => 'US', 'active' => false]),
            row(['id' => 4, 'country' => 'FR', 'active' => false]),
        );

        $joined = $left->joinCross(rows(schema(bool_schema('active')), row(['active' => true])), '_');

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'active' => false, '_active' => true],
                ['id' => 2, 'country' => 'PL', 'active' => false, '_active' => true],
                ['id' => 3, 'country' => 'US', 'active' => false, '_active' => true],
                ['id' => 4, 'country' => 'FR', 'active' => false, '_active' => true],
            ],
            $joined->toArray(),
        );
    }

    public function test_inner_empty(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'PL']),
            row(['id' => 3, 'country' => 'US']),
            row(['id' => 4, 'country' => 'FR']),
        );

        $joined = $left->joinInner(rows(schema()), Expression::on(['country' => 'code']));

        static::assertEquals(rows(schema(int_schema('id'), str_schema('country'))), $joined);
    }

    public function test_inner_join(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'PL']),
            row(['id' => 3, 'country' => 'US']),
            row(['id' => 4, 'country' => 'FR']),
        );

        $joined = $left->joinInner(
            rows(
                schema(str_schema('code'), str_schema('name')),
                row(['code' => 'PL', 'name' => 'Poland']),
                row(['code' => 'US', 'name' => 'United States']),
                row(['code' => 'GB', 'name' => 'Great Britain']),
            ),
            join_on(['country' => 'code'], 'joined_'),
        );

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 3, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
            ],
            $joined->toArray(),
        );
    }

    public function test_inner_join_emits_every_matching_right_row(): void
    {
        $joined = rows(schema(int_schema('id'), str_schema('country')), row(['id' => 1, 'country' => 'PL']))->joinInner(
            rows(
                schema(int_schema('code'), str_schema('city')),
                row(['code' => 1, 'city' => 'Warsaw']),
                row(['code' => 1, 'city' => 'Cracow']),
            ),
            join_on(['id' => 'code']),
        );

        static::assertSame(
            [
                ['id' => 1, 'country' => 'PL', 'code' => 1, 'city' => 'Warsaw'],
                ['id' => 1, 'country' => 'PL', 'code' => 1, 'city' => 'Cracow'],
            ],
            $joined->toArray(),
        );
    }

    public function test_inner_join_into_empty(): void
    {
        $left = rows(schema());

        $joined = $left->joinInner(
            rows(
                schema(str_schema('code'), str_schema('name')),
                row(['code' => 'PL', 'name' => 'Poland']),
                row(['code' => 'US', 'name' => 'United States']),
                row(['code' => 'GB', 'name' => 'Great Britain']),
            ),
            Expression::on(['country' => 'code']),
        );

        static::assertEquals(rows(schema(str_schema('code'), str_schema('name'))), $joined);
    }

    public function test_inner_join_with_duplicated_entries(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage(
            'Entry definitions must be unique, duplicated entries: [id], all: [id, country, id, code, name]',
        );

        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'PL']),
            row(['id' => 3, 'country' => 'US']),
            row(['id' => 4, 'country' => 'FR']),
        );

        $left->joinInner(
            rows(
                schema(int_schema('id'), str_schema('code'), str_schema('name')),
                row(['id' => 101, 'code' => 'PL', 'name' => 'Poland']),
                row(['id' => 102, 'code' => 'US', 'name' => 'United States']),
                row(['id' => 103, 'code' => 'GB', 'name' => 'Great Britain']),
            ),
            Expression::on(['country' => 'code'], joinPrefix: ''),
        );
    }

    public function test_inner_join_without_prefix(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country_code')),
            row(['id' => 1, 'country_code' => 'PL']),
            row(['id' => 2, 'country_code' => 'PL']),
            row(['id' => 3, 'country_code' => 'US']),
            row(['id' => 4, 'country_code' => 'FR']),
        );

        $joined = $left->joinInner(
            rows(
                schema(str_schema('country_code'), str_schema('name')),
                row(['country_code' => 'PL', 'name' => 'Poland']),
                row(['country_code' => 'US', 'name' => 'United States']),
                row(['country_code' => 'GB', 'name' => 'Great Britain']),
            ),
            join_on(['country_code' => 'country_code']),
        );

        static::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 3, 'country_code' => 'US', 'name' => 'United States'],
            ],
            $joined->toArray(),
        );
    }

    public function test_left_anti_join(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'US']),
            row(['id' => 3, 'country' => 'FR']),
        );

        $joined = $left->joinLeftAnti(
            rows(
                schema(str_schema('code'), str_schema('name')),
                row(['code' => 'US', 'name' => 'United States']),
                row(['code' => 'FR', 'name' => 'France']),
            ),
            Expression::on(['country' => 'code']),
        );

        static::assertEquals(
            rows(schema(int_schema('id'), str_schema('country')), row(['id' => 1, 'country' => 'PL'])),
            $joined,
        );
    }

    public function test_left_anti_join_on_empty(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'US']),
            row(['id' => 3, 'country' => 'FR']),
        );

        $joined = $left->joinLeftAnti(rows(schema()), Expression::on(['country' => 'code']));

        static::assertEquals($left, $joined);
    }

    public function test_left_anti_join_without_prefix(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country_code')),
            row(['id' => 1, 'country_code' => 'PL']),
            row(['id' => 2, 'country_code' => 'US']),
            row(['id' => 3, 'country_code' => 'FR']),
        );

        $joined = $left->joinLeftAnti(
            rows(
                schema(str_schema('country_code'), str_schema('name')),
                row(['country_code' => 'US', 'name' => 'United States']),
                row(['country_code' => 'FR', 'name' => 'France']),
            ),
            Expression::on(['country_code' => 'country_code']),
        );

        static::assertEquals(
            rows(schema(int_schema('id'), str_schema('country_code')), row(['id' => 1, 'country_code' => 'PL'])),
            $joined,
        );
    }

    public function test_left_join(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'US']),
            row(['id' => 3, 'country' => 'FR']),
        );

        $joined = $left->joinLeft(
            rows(
                schema(str_schema('code'), str_schema('name')),
                row(['code' => 'PL', 'name' => 'Poland']),
                row(['code' => 'US', 'name' => 'United States']),
                row(['code' => 'GB', 'name' => 'Great Britain']),
            ),
            Expression::on(['country' => 'code'], 'joined_'),
        );

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 3, 'country' => 'FR', 'joined_code' => null, 'joined_name' => null],
            ],
            $joined->toArray(),
        );
    }

    public function test_left_join_empty(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'US']),
            row(['id' => 3, 'country' => 'FR']),
        );

        $joined = $left->joinLeft(rows(schema()), Expression::on(['country' => 'code']));

        static::assertEquals(
            rows(
                schema(int_schema('id'), str_schema('country')),
                row(['id' => 1, 'country' => 'PL']),
                row(['id' => 2, 'country' => 'US']),
                row(['id' => 3, 'country' => 'FR']),
            ),
            $joined,
        );
    }

    public function test_left_join_empty_without_prefix(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country_code')),
            row(['id' => 1, 'country_code' => 'PL']),
            row(['id' => 2, 'country_code' => 'US']),
            row(['id' => 3, 'country_code' => 'FR']),
        );

        $joined = $left->joinLeft(rows(schema()), Expression::on(['country_code' => 'country_code']));

        static::assertEquals(
            rows(
                schema(int_schema('id'), str_schema('country_code')),
                row(['id' => 1, 'country_code' => 'PL']),
                row(['id' => 2, 'country_code' => 'US']),
                row(['id' => 3, 'country_code' => 'FR']),
            ),
            $joined,
        );
    }

    public function test_left_join_to_empty(): void
    {
        $left = rows(schema());

        $joined = $left->joinLeft(
            rows(
                schema(str_schema('code'), str_schema('name')),
                row(['code' => 'PL', 'name' => 'Poland']),
                row(['code' => 'US', 'name' => 'United States']),
                row(['code' => 'GB', 'name' => 'Great Britain']),
            ),
            Expression::on(['country' => 'code']),
        );

        static::assertEquals(
            rows(schema(str_schema('code', nullable: true), str_schema('name', nullable: true))),
            $joined,
        );
    }

    public function test_left_join_with_the_duplicated_columns(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage(
            'Entry definitions must be unique, duplicated entries: [id], all: [id, country, id, code, name]',
        );

        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'US']),
            row(['id' => 3, 'country' => 'FR']),
        );

        $left->joinLeft(
            rows(
                schema(int_schema('id'), str_schema('code'), str_schema('name')),
                row(['id' => 100, 'code' => 'PL', 'name' => 'Poland']),
                row(['id' => 101, 'code' => 'US', 'name' => 'United States']),
                row(['id' => 102, 'code' => 'GB', 'name' => 'Great Britain']),
            ),
            Expression::on(['country' => 'code'], ''),
        );
    }

    public function test_left_join_without_prefix(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country_code')),
            row(['id' => 1, 'country_code' => 'PL']),
            row(['id' => 2, 'country_code' => 'US']),
            row(['id' => 3, 'country_code' => 'FR']),
        );

        $joined = $left->joinLeft(
            rows(
                schema(str_schema('country_code'), str_schema('name')),
                row(['country_code' => 'PL', 'name' => 'Poland']),
                row(['country_code' => 'US', 'name' => 'United States']),
                row(['country_code' => 'GB', 'name' => 'Great Britain']),
            ),
            Expression::on(['country_code' => 'country_code']),
        );

        static::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => 3, 'country_code' => 'FR', 'name' => null],
            ],
            $joined->toArray(),
        );
    }

    public function test_right_join(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'PL']),
            row(['id' => 3, 'country' => 'US']),
            row(['id' => 4, 'country' => 'FR']),
        );

        $joined = $left->joinRight(
            rows(
                schema(str_schema('code'), str_schema('name')),
                row(['code' => 'PL', 'name' => 'Poland']),
                row(['code' => 'US', 'name' => 'United States']),
                row(['code' => 'GB', 'name' => 'Great Britain']),
            ),
            Expression::on(['country' => 'code'], 'joined_'),
        );

        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 3, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => null, 'country' => null, 'joined_code' => 'GB', 'joined_name' => 'Great Britain'],
            ],
            $joined->toArray(),
        );
    }

    public function test_right_join_empty(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'PL']),
            row(['id' => 3, 'country' => 'US']),
            row(['id' => 4, 'country' => 'FR']),
        );

        $joined = $left->joinRight(rows(schema()), Expression::on(['country' => 'code']));

        static::assertEquals(
            rows(schema(int_schema('id', nullable: true), str_schema('country', nullable: true))),
            $joined,
        );
    }

    public function test_right_join_to_empty(): void
    {
        $left = rows(schema());

        $joined = $left->joinRight(
            rows(
                schema(str_schema('code'), str_schema('name')),
                row(['code' => 'PL', 'name' => 'Poland']),
                row(['code' => 'US', 'name' => 'United States']),
                row(['code' => 'GB', 'name' => 'Great Britain']),
            ),
            Expression::on(['country' => 'code'], 'joined_'),
        );

        static::assertEquals(
            [
                ['joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['joined_code' => 'US', 'joined_name' => 'United States'],
                ['joined_code' => 'GB', 'joined_name' => 'Great Britain'],
            ],
            $joined->toArray(),
        );
    }

    public function test_right_join_with_duplicated_entry_names(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage(
            'Entry definitions must be unique, duplicated entries: [id], all: [id, country, id, code, name]',
        );

        $left = rows(
            schema(int_schema('id'), str_schema('country')),
            row(['id' => 1, 'country' => 'PL']),
            row(['id' => 2, 'country' => 'PL']),
            row(['id' => 3, 'country' => 'US']),
            row(['id' => 4, 'country' => 'FR']),
        );

        $left->joinRight(
            rows(
                schema(int_schema('id'), str_schema('code'), str_schema('name')),
                row(['id' => 101, 'code' => 'PL', 'name' => 'Poland']),
                row(['id' => 102, 'code' => 'US', 'name' => 'United States']),
                row(['id' => 103, 'code' => 'GB', 'name' => 'Great Britain']),
            ),
            Expression::on(['country' => 'code'], ''),
        );
    }

    public function test_right_join_without_prefix(): void
    {
        $left = rows(
            schema(int_schema('id'), str_schema('country_code')),
            row(['id' => 1, 'country_code' => 'PL']),
            row(['id' => 2, 'country_code' => 'PL']),
            row(['id' => 3, 'country_code' => 'US']),
            row(['id' => 4, 'country_code' => 'FR']),
        );

        $joined = $left->joinRight(
            rows(
                schema(str_schema('country_code'), str_schema('name')),
                row(['country_code' => 'PL', 'name' => 'Poland']),
                row(['country_code' => 'US', 'name' => 'United States']),
                row(['country_code' => 'GB', 'name' => 'Great Britain']),
            ),
            Expression::on(['country_code' => 'country_code']),
        );

        static::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 3, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => null, 'country_code' => 'GB', 'name' => 'Great Britain'],
            ],
            $joined->toArray(),
        );
    }
}
