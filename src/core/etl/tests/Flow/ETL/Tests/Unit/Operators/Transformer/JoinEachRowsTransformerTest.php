<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Transformer;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\DataDependentSchemaException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\ExtractorFactory;
use Flow\ETL\Operators\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Tests\Double\StaticExtractorFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JoinEachRowsTransformerTest extends FlowTestCase
{
    public function test_bind_refuses_to_derive_a_data_dependent_schema(): void
    {
        $right = new class implements ExtractorFactory {
            public function from(Rows $rows): Extractor
            {
                return from_rows($rows);
            }
        };

        $this->expectException(DataDependentSchemaException::class);
        $this->expectExceptionMessage(
            "cannot describe its output before rows flow: its right side is built from each left batch's row values",
        );

        JoinEachRowsTransformer::inner($right, Expression::on(['id' => 'id']))->bind(schema(int_schema('id')));
    }

    public function test_inner_join_rows(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 2, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
            ],
            JoinEachRowsTransformer::inner(
                new class implements ExtractorFactory {
                    public function from(Rows $rows): Extractor
                    {
                        return from_rows(array_to_rows(
                            [
                                ['code' => 'PL', 'name' => 'Poland'],
                                ['code' => 'US', 'name' => 'United States'],
                                ['code' => 'GB', 'name' => 'Great Britain'],
                            ],
                            schema(str_schema('code'), str_schema('name')),
                        ));
                    }
                },
                Expression::on(['country' => 'code'], 'joined_'),
            )
                ->transform(
                    array_to_rows(
                        [
                            ['id' => 1, 'country' => 'PL'],
                            ['id' => 2, 'country' => 'US'],
                            ['id' => 3, 'country' => 'FR'],
                        ],
                        schema(int_schema('id'), str_schema('country')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_left_join_rows(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_name' => 'Poland', 'joined_code' => 'PL'],
                ['id' => 2, 'country' => 'US', 'joined_name' => 'United States', 'joined_code' => 'US'],
                ['id' => 3, 'country' => 'FR', 'joined_name' => null, 'joined_code' => null],
            ],
            JoinEachRowsTransformer::left(
                new class implements ExtractorFactory {
                    public function from(Rows $rows): Extractor
                    {
                        return from_rows(array_to_rows(
                            [
                                ['code' => 'PL', 'name' => 'Poland'],
                                ['code' => 'US', 'name' => 'United States'],
                                ['code' => 'GB', 'name' => 'Great Britain'],
                            ],
                            schema(str_schema('code'), str_schema('name')),
                        ));
                    }
                },
                Expression::on(['country' => 'code'], 'joined_'),
            )
                ->transform(
                    array_to_rows(
                        [
                            ['id' => 1, 'country' => 'PL'],
                            ['id' => 2, 'country' => 'US'],
                            ['id' => 3, 'country' => 'FR'],
                        ],
                        schema(int_schema('id'), str_schema('country')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_right_join_rows(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => null, 'country' => null, 'joined_code' => 'GB', 'joined_name' => 'Great Britain'],
            ],
            JoinEachRowsTransformer::right(
                new class implements ExtractorFactory {
                    public function from(Rows $rows): Extractor
                    {
                        return from_rows(array_to_rows(
                            [
                                ['code' => 'PL', 'name' => 'Poland'],
                                ['code' => 'US', 'name' => 'United States'],
                                ['code' => 'GB', 'name' => 'Great Britain'],
                            ],
                            schema(str_schema('code'), str_schema('name')),
                        ));
                    }
                },
                Expression::on(['country' => 'code'], 'joined_'),
            )
                ->transform(
                    array_to_rows(
                        [
                            ['id' => 1, 'country' => 'PL'],
                            ['id' => 2, 'country' => 'US'],
                            ['id' => 3, 'country' => 'FR'],
                        ],
                        schema(int_schema('id'), str_schema('country')),
                    ),
                    flow_context(config())->backend(),
                )
                ->toArray(),
        );
    }

    public function test_does_not_describe(): void
    {
        static::assertNull(
            JoinEachRowsTransformer::inner(
                new class implements ExtractorFactory {
                    public function from(Rows $rows): Extractor
                    {
                        return from_rows($rows);
                    }
                },
                Expression::on(['id' => 'id']),
            )->describe(),
        );
    }

    public function test_inner_empty(): void
    {
        static::assertEquals(
            rows(schema(int_schema('id'), str_schema('country'))),
            JoinEachRowsTransformer::inner(new StaticExtractorFactory(from_rows(rows(schema()))), Expression::on([
                'country' => 'code',
            ]))->transform(array_to_rows([
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ], schema(int_schema('id'), str_schema('country'))), flow_context()->backend()),
        );
    }

    public function test_inner_join(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 3, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
            ],
            JoinEachRowsTransformer::inner(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                ))),
                join_on(['country' => 'code'], 'joined_'),
            )
                ->transform(
                    array_to_rows(
                        [
                            ['id' => 1, 'country' => 'PL'],
                            ['id' => 2, 'country' => 'PL'],
                            ['id' => 3, 'country' => 'US'],
                            ['id' => 4, 'country' => 'FR'],
                        ],
                        schema(int_schema('id'), str_schema('country')),
                    ),
                    flow_context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_inner_join_emits_every_matching_right_row(): void
    {
        static::assertSame(
            [
                ['id' => 1, 'country' => 'PL', 'code' => 1, 'city' => 'Warsaw'],
                ['id' => 1, 'country' => 'PL', 'code' => 1, 'city' => 'Cracow'],
            ],
            JoinEachRowsTransformer::inner(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [['code' => 1, 'city' => 'Warsaw'], ['code' => 1, 'city' => 'Cracow']],
                    schema(int_schema('code'), str_schema('city')),
                ))),
                join_on(['id' => 'code']),
            )
                ->transform(
                    array_to_rows([['id' => 1, 'country' => 'PL']], schema(int_schema('id'), str_schema('country'))),
                    flow_context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_inner_join_into_empty(): void
    {
        static::assertEquals(
            rows(schema(str_schema('code'), str_schema('name'))),
            JoinEachRowsTransformer::inner(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                ))),
                Expression::on(['country' => 'code']),
            )->transform(rows(schema()), flow_context()->backend()),
        );
    }

    public function test_inner_join_with_duplicated_entries(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage(
            'Column definitions must be unique, duplicated columns: [id], all: [id, country, id, code, name]',
        );

        JoinEachRowsTransformer::inner(
            new StaticExtractorFactory(from_rows(array_to_rows(
                [
                    ['id' => 101, 'code' => 'PL', 'name' => 'Poland'],
                    ['id' => 102, 'code' => 'US', 'name' => 'United States'],
                    ['id' => 103, 'code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(int_schema('id'), str_schema('code'), str_schema('name')),
            ))),
            Expression::on(['country' => 'code'], joinPrefix: ''),
        )->transform(
            array_to_rows(
                [
                    ['id' => 1, 'country' => 'PL'],
                    ['id' => 2, 'country' => 'PL'],
                    ['id' => 3, 'country' => 'US'],
                    ['id' => 4, 'country' => 'FR'],
                ],
                schema(int_schema('id'), str_schema('country')),
            ),
            flow_context()->backend(),
        );
    }

    public function test_inner_join_without_prefix(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 3, 'country_code' => 'US', 'name' => 'United States'],
            ],
            JoinEachRowsTransformer::inner(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [
                        ['country_code' => 'PL', 'name' => 'Poland'],
                        ['country_code' => 'US', 'name' => 'United States'],
                        ['country_code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('country_code'), str_schema('name')),
                ))),
                join_on(['country_code' => 'country_code']),
            )
                ->transform(
                    array_to_rows(
                        [
                            ['id' => 1, 'country_code' => 'PL'],
                            ['id' => 2, 'country_code' => 'PL'],
                            ['id' => 3, 'country_code' => 'US'],
                            ['id' => 4, 'country_code' => 'FR'],
                        ],
                        schema(int_schema('id'), str_schema('country_code')),
                    ),
                    flow_context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_left_anti_join(): void
    {
        static::assertEquals(
            array_to_rows([['id' => 1, 'country' => 'PL']], schema(int_schema('id'), str_schema('country'))),
            JoinEachRowsTransformer::leftAnti(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [['code' => 'US', 'name' => 'United States'], ['code' => 'FR', 'name' => 'France']],
                    schema(str_schema('code'), str_schema('name')),
                ))),
                Expression::on(['country' => 'code']),
            )->transform(
                array_to_rows(
                    [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
                    schema(int_schema('id'), str_schema('country')),
                ),
                flow_context()->backend(),
            ),
        );
    }

    public function test_left_anti_join_on_empty(): void
    {
        $left = array_to_rows(
            [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
            schema(int_schema('id'), str_schema('country')),
        );

        static::assertEquals($left, JoinEachRowsTransformer::leftAnti(
            new StaticExtractorFactory(from_rows(rows(schema()))),
            Expression::on(['country' => 'code']),
        )->transform($left, flow_context()->backend()));
    }

    public function test_left_anti_join_without_prefix(): void
    {
        static::assertEquals(
            array_to_rows([['id' => 1, 'country_code' => 'PL']], schema(int_schema('id'), str_schema('country_code'))),
            JoinEachRowsTransformer::leftAnti(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [['country_code' => 'US', 'name' => 'United States'], ['country_code' => 'FR', 'name' => 'France']],
                    schema(str_schema('country_code'), str_schema('name')),
                ))),
                Expression::on(['country_code' => 'country_code']),
            )->transform(
                array_to_rows(
                    [
                        ['id' => 1, 'country_code' => 'PL'],
                        ['id' => 2, 'country_code' => 'US'],
                        ['id' => 3, 'country_code' => 'FR'],
                    ],
                    schema(int_schema('id'), str_schema('country_code')),
                ),
                flow_context()->backend(),
            ),
        );
    }

    public function test_left_join(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => 3, 'country' => 'FR', 'joined_code' => null, 'joined_name' => null],
            ],
            JoinEachRowsTransformer::left(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                ))),
                Expression::on(['country' => 'code'], 'joined_'),
            )
                ->transform(
                    array_to_rows(
                        [
                            ['id' => 1, 'country' => 'PL'],
                            ['id' => 2, 'country' => 'US'],
                            ['id' => 3, 'country' => 'FR'],
                        ],
                        schema(int_schema('id'), str_schema('country')),
                    ),
                    flow_context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_left_join_empty(): void
    {
        static::assertEquals(
            array_to_rows(
                [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
                schema(int_schema('id'), str_schema('country')),
            ),
            JoinEachRowsTransformer::left(new StaticExtractorFactory(from_rows(rows(schema()))), Expression::on([
                'country' => 'code',
            ]))->transform(array_to_rows([['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']], schema(int_schema('id'), str_schema('country'))), flow_context()->backend()),
        );
    }

    public function test_left_join_empty_without_prefix(): void
    {
        static::assertEquals(
            array_to_rows(
                [
                    ['id' => 1, 'country_code' => 'PL'],
                    ['id' => 2, 'country_code' => 'US'],
                    ['id' => 3, 'country_code' => 'FR'],
                ],
                schema(int_schema('id'), str_schema('country_code')),
            ),
            JoinEachRowsTransformer::left(new StaticExtractorFactory(from_rows(rows(schema()))), Expression::on([
                'country_code' => 'country_code',
            ]))->transform(array_to_rows([
                ['id' => 1, 'country_code' => 'PL'],
                ['id' => 2, 'country_code' => 'US'],
                ['id' => 3, 'country_code' => 'FR'],
            ], schema(int_schema('id'), str_schema('country_code'))), flow_context()->backend()),
        );
    }

    public function test_left_join_to_empty(): void
    {
        static::assertEquals(
            rows(schema(str_schema('code', nullable: true), str_schema('name', nullable: true))),
            JoinEachRowsTransformer::left(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                ))),
                Expression::on(['country' => 'code']),
            )->transform(rows(schema()), flow_context()->backend()),
        );
    }

    public function test_left_join_with_the_duplicated_columns(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage(
            'Column definitions must be unique, duplicated columns: [id], all: [id, country, id, code, name]',
        );

        JoinEachRowsTransformer::left(
            new StaticExtractorFactory(from_rows(array_to_rows(
                [
                    ['id' => 100, 'code' => 'PL', 'name' => 'Poland'],
                    ['id' => 101, 'code' => 'US', 'name' => 'United States'],
                    ['id' => 102, 'code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(int_schema('id'), str_schema('code'), str_schema('name')),
            ))),
            Expression::on(['country' => 'code'], ''),
        )->transform(
            array_to_rows(
                [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'US'], ['id' => 3, 'country' => 'FR']],
                schema(int_schema('id'), str_schema('country')),
            ),
            flow_context()->backend(),
        );
    }

    public function test_left_join_without_prefix(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => 3, 'country_code' => 'FR', 'name' => null],
            ],
            JoinEachRowsTransformer::left(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [
                        ['country_code' => 'PL', 'name' => 'Poland'],
                        ['country_code' => 'US', 'name' => 'United States'],
                        ['country_code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('country_code'), str_schema('name')),
                ))),
                Expression::on(['country_code' => 'country_code']),
            )
                ->transform(
                    array_to_rows(
                        [
                            ['id' => 1, 'country_code' => 'PL'],
                            ['id' => 2, 'country_code' => 'US'],
                            ['id' => 3, 'country_code' => 'FR'],
                        ],
                        schema(int_schema('id'), str_schema('country_code')),
                    ),
                    flow_context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_right_join(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 2, 'country' => 'PL', 'joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['id' => 3, 'country' => 'US', 'joined_code' => 'US', 'joined_name' => 'United States'],
                ['id' => null, 'country' => null, 'joined_code' => 'GB', 'joined_name' => 'Great Britain'],
            ],
            JoinEachRowsTransformer::right(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                ))),
                Expression::on(['country' => 'code'], 'joined_'),
            )
                ->transform(
                    array_to_rows(
                        [
                            ['id' => 1, 'country' => 'PL'],
                            ['id' => 2, 'country' => 'PL'],
                            ['id' => 3, 'country' => 'US'],
                            ['id' => 4, 'country' => 'FR'],
                        ],
                        schema(int_schema('id'), str_schema('country')),
                    ),
                    flow_context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_right_join_empty(): void
    {
        static::assertEquals(
            rows(schema(int_schema('id', nullable: true), str_schema('country', nullable: true))),
            JoinEachRowsTransformer::right(new StaticExtractorFactory(from_rows(rows(schema()))), Expression::on([
                'country' => 'code',
            ]))->transform(array_to_rows([
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'PL'],
                ['id' => 3, 'country' => 'US'],
                ['id' => 4, 'country' => 'FR'],
            ], schema(int_schema('id'), str_schema('country'))), flow_context()->backend()),
        );
    }

    public function test_right_join_to_empty(): void
    {
        static::assertEquals(
            [
                ['joined_code' => 'PL', 'joined_name' => 'Poland'],
                ['joined_code' => 'US', 'joined_name' => 'United States'],
                ['joined_code' => 'GB', 'joined_name' => 'Great Britain'],
            ],
            JoinEachRowsTransformer::right(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [
                        ['code' => 'PL', 'name' => 'Poland'],
                        ['code' => 'US', 'name' => 'United States'],
                        ['code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('code'), str_schema('name')),
                ))),
                Expression::on(['country' => 'code'], 'joined_'),
            )
                ->transform(rows(schema()), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_right_join_with_duplicated_entry_names(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage(
            'Column definitions must be unique, duplicated columns: [id], all: [id, country, id, code, name]',
        );

        JoinEachRowsTransformer::right(
            new StaticExtractorFactory(from_rows(array_to_rows(
                [
                    ['id' => 101, 'code' => 'PL', 'name' => 'Poland'],
                    ['id' => 102, 'code' => 'US', 'name' => 'United States'],
                    ['id' => 103, 'code' => 'GB', 'name' => 'Great Britain'],
                ],
                schema(int_schema('id'), str_schema('code'), str_schema('name')),
            ))),
            Expression::on(['country' => 'code'], ''),
        )->transform(
            array_to_rows(
                [
                    ['id' => 1, 'country' => 'PL'],
                    ['id' => 2, 'country' => 'PL'],
                    ['id' => 3, 'country' => 'US'],
                    ['id' => 4, 'country' => 'FR'],
                ],
                schema(int_schema('id'), str_schema('country')),
            ),
            flow_context()->backend(),
        );
    }

    public function test_right_join_without_prefix(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 2, 'country_code' => 'PL', 'name' => 'Poland'],
                ['id' => 3, 'country_code' => 'US', 'name' => 'United States'],
                ['id' => null, 'country_code' => 'GB', 'name' => 'Great Britain'],
            ],
            JoinEachRowsTransformer::right(
                new StaticExtractorFactory(from_rows(array_to_rows(
                    [
                        ['country_code' => 'PL', 'name' => 'Poland'],
                        ['country_code' => 'US', 'name' => 'United States'],
                        ['country_code' => 'GB', 'name' => 'Great Britain'],
                    ],
                    schema(str_schema('country_code'), str_schema('name')),
                ))),
                Expression::on(['country_code' => 'country_code']),
            )
                ->transform(
                    array_to_rows(
                        [
                            ['id' => 1, 'country_code' => 'PL'],
                            ['id' => 2, 'country_code' => 'PL'],
                            ['id' => 3, 'country_code' => 'US'],
                            ['id' => 4, 'country_code' => 'FR'],
                        ],
                        schema(int_schema('id'), str_schema('country_code')),
                    ),
                    flow_context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_a_corrupted_record_of_the_right_side_is_refused(): void
    {
        $this->expectException(CorruptedRecordException::class);

        JoinEachRowsTransformer::inner(
            new StaticExtractorFactory(from_array([['code' => 'x']], schema(int_schema('code')))),
            Expression::on(['id' => 'code']),
        )->transform(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context(config())->backend());
    }
}
