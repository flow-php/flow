<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\RowsJoin;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowsJoinTest extends FlowTestCase
{
    public function test_cross_join(): void
    {
        $left = array_to_rows(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->cross($left, array_to_rows(
            [['num' => 1, 'active' => true], ['num' => 2, 'active' => false]],
            schema(int_schema('num'), bool_schema('active')),
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
        $left = array_to_rows(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country')),
        );

        // n x 0 = 0, and a zero-row batch fits the cross schema vacuously
        static::assertSame(
            [],
            (new RowsJoin(new AdaptiveBackend()))
                ->cross($left, rows(schema()))
                ->toArray(),
        );
    }

    public function test_cross_join_with_an_empty_side_yields_no_rows_under_the_cross_schema(): void
    {
        // a cross join with an empty side produces zero rows, and the batch still declares both
        // sides' columns - the schema describes the plan, not the cardinality
        $emptyRight = (new RowsJoin(new AdaptiveBackend()))->cross(
            array_to_rows([['id' => 1, 'country' => 'PL']], schema(int_schema('id'), str_schema('country'))),
            rows(schema(str_schema('code'))),
        );

        static::assertSame(['id', 'country', 'joined_code'], $emptyRight->schema()->references()->names());
        static::assertSame([], $emptyRight->toArray());

        $emptyLeft = (new RowsJoin(new AdaptiveBackend()))->cross(
            rows(schema(int_schema('id'))),
            array_to_rows([['code' => 'PL']], schema(str_schema('code'))),
        );

        static::assertSame(['id', 'joined_code'], $emptyLeft->schema()->references()->names());
        static::assertSame([], $emptyLeft->toArray());
    }

    public function test_cross_join_left_empty(): void
    {
        $left = rows(schema());

        $joined = (new RowsJoin(new AdaptiveBackend()))->cross($left, array_to_rows(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country')),
        ));

        static::assertSame([], $joined->toArray());
    }

    public function test_cross_join_left_with_name_conflict(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage(
            'Entry definitions must be unique, duplicated entries: [active], all: [id, country, active, active]',
        );

        $left = array_to_rows(
            [
                ['id' => 1, 'country' => 'PL', 'active' => false],
                ['id' => 2, 'country' => 'PL', 'active' => false],
                ['id' => 3, 'country' => 'US', 'active' => false],
                ['id' => 4, 'country' => 'FR', 'active' => false],
            ],
            schema(int_schema('id'), str_schema('country'), bool_schema('active')),
        );

        (new RowsJoin(new AdaptiveBackend()))->cross(
            $left,
            array_to_rows([['active' => true]], schema(bool_schema('active'))),
            '',
        );
    }

    public function test_cross_join_left_with_name_conflict_with_prefix(): void
    {
        $left = array_to_rows(
            [
                ['id' => 1, 'country' => 'PL', 'active' => false],
                ['id' => 2, 'country' => 'PL', 'active' => false],
                ['id' => 3, 'country' => 'US', 'active' => false],
                ['id' => 4, 'country' => 'FR', 'active' => false],
            ],
            schema(int_schema('id'), str_schema('country'), bool_schema('active')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->cross(
            $left,
            array_to_rows([['active' => true]], schema(bool_schema('active'))),
            '_',
        );

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
        $left = array_to_rows(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->inner(
            $left,
            rows(schema()),
            Expression::on(['country' => 'code']),
        );

        static::assertEquals(rows(schema(int_schema('id'), str_schema('country'))), $joined);
    }

    public function test_inner_join(): void
    {
        $left = array_to_rows(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->inner(
            $left,
            array_to_rows(
                [
                    ['code' => 'PL', 'name' => 'Poland'],
                    ['code' => 'US', 'name' => 'United States'],
                    ['code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(str_schema('code'), str_schema('name')),
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
        $joined = (new RowsJoin(new AdaptiveBackend()))->inner(
            array_to_rows([['id' => 1, 'country' => 'PL']], schema(int_schema('id'), str_schema('country'))),
            array_to_rows(
                [['code' => 1, 'city' => 'Warsaw'], ['code' => 1, 'city' => 'Cracow']],
                schema(int_schema('code'), str_schema('city')),
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

        $joined = (new RowsJoin(new AdaptiveBackend()))->inner(
            $left,
            array_to_rows(
                [
                    ['code' => 'PL', 'name' => 'Poland'],
                    ['code' => 'US', 'name' => 'United States'],
                    ['code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(str_schema('code'), str_schema('name')),
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

        $left = array_to_rows(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country')),
        );

        (new RowsJoin(new AdaptiveBackend()))->inner(
            $left,
            array_to_rows(
                [
                    ['id' => 101, 'code' => 'PL', 'name' => 'Poland'],
                    ['id' => 102, 'code' => 'US', 'name' => 'United States'],
                    ['id' => 103, 'code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(int_schema('id'), str_schema('code'), str_schema('name')),
            ),
            Expression::on(['country' => 'code'], joinPrefix: ''),
        );
    }

    public function test_inner_join_without_prefix(): void
    {
        $left = array_to_rows(
            [
                ['id' => 1, 'country_code' => 'PL'],
                ['id' => 2, 'country_code' => 'PL'],
                ['id' => 3, 'country_code' => 'US'],
                ['id' => 4, 'country_code' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country_code')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->inner(
            $left,
            array_to_rows(
                [
                    ['country_code' => 'PL', 'name' => 'Poland'],
                    ['country_code' => 'US', 'name' => 'United States'],
                    ['country_code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(str_schema('country_code'), str_schema('name')),
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
        $left = array_to_rows(
            [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
            schema(int_schema('id'), str_schema('country')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->leftAnti(
            $left,
            array_to_rows(
                [['code' => 'US', 'name' => 'United States'], ['code' => 'FR', 'name' => 'France']],
                schema(str_schema('code'), str_schema('name')),
            ),
            Expression::on(['country' => 'code']),
        );

        static::assertEquals(
            array_to_rows([['id' => 1, 'country' => 'PL']], schema(int_schema('id'), str_schema('country'))),
            $joined,
        );
    }

    public function test_left_anti_join_on_empty(): void
    {
        $left = array_to_rows(
            [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
            schema(int_schema('id'), str_schema('country')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->leftAnti(
            $left,
            rows(schema()),
            Expression::on(['country' => 'code']),
        );

        static::assertEquals($left, $joined);
    }

    public function test_left_anti_join_without_prefix(): void
    {
        $left = array_to_rows(
            [
                ['id' => 1, 'country_code' => 'PL'],
                ['id' => 2, 'country_code' => 'US'],
                ['id' => 3, 'country_code' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country_code')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->leftAnti(
            $left,
            array_to_rows(
                [['country_code' => 'US', 'name' => 'United States'], ['country_code' => 'FR', 'name' => 'France']],
                schema(str_schema('country_code'), str_schema('name')),
            ),
            Expression::on(['country_code' => 'country_code']),
        );

        static::assertEquals(
            array_to_rows([['id' => 1, 'country_code' => 'PL']], schema(int_schema('id'), str_schema('country_code'))),
            $joined,
        );
    }

    public function test_left_join(): void
    {
        $left = array_to_rows(
            [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
            schema(int_schema('id'), str_schema('country')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->left(
            $left,
            array_to_rows(
                [
                    ['code' => 'PL', 'name' => 'Poland'],
                    ['code' => 'US', 'name' => 'United States'],
                    ['code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(str_schema('code'), str_schema('name')),
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
        $left = array_to_rows(
            [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
            schema(int_schema('id'), str_schema('country')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->left(
            $left,
            rows(schema()),
            Expression::on(['country' => 'code']),
        );

        static::assertEquals(
            array_to_rows(
                [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
                schema(int_schema('id'), str_schema('country')),
            ),
            $joined,
        );
    }

    public function test_left_join_empty_without_prefix(): void
    {
        $left = array_to_rows(
            [
                ['id' => 1, 'country_code' => 'PL'],
                ['id' => 2, 'country_code' => 'US'],
                ['id' => 3, 'country_code' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country_code')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->left(
            $left,
            rows(schema()),
            Expression::on(['country_code' => 'country_code']),
        );

        static::assertEquals(
            array_to_rows(
                [
                    ['id' => 1, 'country_code' => 'PL'],
                    ['id' => 2, 'country_code' => 'US'],
                    ['id' => 3, 'country_code' => 'FR'],
                ],
                schema(int_schema('id'), str_schema('country_code')),
            ),
            $joined,
        );
    }

    public function test_left_join_to_empty(): void
    {
        $left = rows(schema());

        $joined = (new RowsJoin(new AdaptiveBackend()))->left(
            $left,
            array_to_rows(
                [
                    ['code' => 'PL', 'name' => 'Poland'],
                    ['code' => 'US', 'name' => 'United States'],
                    ['code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(str_schema('code'), str_schema('name')),
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

        $left = array_to_rows(
            [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
            schema(int_schema('id'), str_schema('country')),
        );

        (new RowsJoin(new AdaptiveBackend()))->left(
            $left,
            array_to_rows(
                [
                    ['id' => 100, 'code' => 'PL', 'name' => 'Poland'],
                    ['id' => 101, 'code' => 'US', 'name' => 'United States'],
                    ['id' => 102, 'code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(int_schema('id'), str_schema('code'), str_schema('name')),
            ),
            Expression::on(['country' => 'code'], ''),
        );
    }

    public function test_left_join_without_prefix(): void
    {
        $left = array_to_rows(
            [
                ['id' => 1, 'country_code' => 'PL'],
                ['id' => 2, 'country_code' => 'US'],
                ['id' => 3, 'country_code' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country_code')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->left(
            $left,
            array_to_rows(
                [
                    ['country_code' => 'PL', 'name' => 'Poland'],
                    ['country_code' => 'US', 'name' => 'United States'],
                    ['country_code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(str_schema('country_code'), str_schema('name')),
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
        $left = array_to_rows(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->right(
            $left,
            array_to_rows(
                [
                    ['code' => 'PL', 'name' => 'Poland'],
                    ['code' => 'US', 'name' => 'United States'],
                    ['code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(str_schema('code'), str_schema('name')),
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
        $left = array_to_rows(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->right(
            $left,
            rows(schema()),
            Expression::on(['country' => 'code']),
        );

        static::assertEquals(
            rows(schema(int_schema('id', nullable: true), str_schema('country', nullable: true))),
            $joined,
        );
    }

    public function test_right_join_to_empty(): void
    {
        $left = rows(schema());

        $joined = (new RowsJoin(new AdaptiveBackend()))->right(
            $left,
            array_to_rows(
                [
                    ['code' => 'PL', 'name' => 'Poland'],
                    ['code' => 'US', 'name' => 'United States'],
                    ['code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(str_schema('code'), str_schema('name')),
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

        $left = array_to_rows(
            [
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country')),
        );

        (new RowsJoin(new AdaptiveBackend()))->right(
            $left,
            array_to_rows(
                [
                    ['id' => 101, 'code' => 'PL', 'name' => 'Poland'],
                    ['id' => 102, 'code' => 'US', 'name' => 'United States'],
                    ['id' => 103, 'code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(int_schema('id'), str_schema('code'), str_schema('name')),
            ),
            Expression::on(['country' => 'code'], ''),
        );
    }

    public function test_right_join_without_prefix(): void
    {
        $left = array_to_rows(
            [
                ['id' => 1, 'country_code' => 'PL'],
                ['id' => 2, 'country_code' => 'PL'],
                ['id' => 3, 'country_code' => 'US'],
                ['id' => 4, 'country_code' => 'FR'],
            ],
            schema(int_schema('id'), str_schema('country_code')),
        );

        $joined = (new RowsJoin(new AdaptiveBackend()))->right(
            $left,
            array_to_rows(
                [
                    ['country_code' => 'PL', 'name' => 'Poland'],
                    ['country_code' => 'US', 'name' => 'United States'],
                    ['country_code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(str_schema('country_code'), str_schema('name')),
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
