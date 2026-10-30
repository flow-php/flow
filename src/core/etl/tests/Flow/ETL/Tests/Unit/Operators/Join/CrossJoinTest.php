<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Join;

use Flow\ETL\Foundation\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\Join\CrossJoin;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;
use Generator;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class CrossJoinTest extends FlowTestCase
{
    public function test_bind_concatenates_the_left_and_the_right_schema(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            (new CrossJoin())->bind(schema(int_schema('id')), schema(str_schema('name')))->output,
        );
    }

    public function test_bind_prefixes_every_right_side_column(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('right_name')),
            (new CrossJoin('right_'))->bind(schema(int_schema('id')), schema(str_schema('name')))->output,
        );
    }

    public function test_cross_join(): void
    {
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
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    (new CrossJoin('joined_'))->join(
                        (static function () {
                            yield array_to_rows(
                                [
                                    ['id' => 1, 'country' => 'PL'],
                                    ['id' => 2, 'country' => 'PL'],
                                    ['id' => 3, 'country' => 'US'],
                                    ['id' => 4, 'country' => 'FR'],
                                ],
                                schema(int_schema('id'), str_schema('country')),
                            );
                        })(),
                        (static function () {
                            yield array_to_rows(
                                [['num' => 1, 'active' => true], ['num' => 2, 'active' => false]],
                                schema(int_schema('num'), bool_schema('active')),
                            );
                        })(),
                        flow_context()->backend(),
                    ),
                    false,
                ),
            )[0],
        );
    }

    public function test_the_right_side_is_read_once_over_every_left_batch(): void
    {
        $reads = 0;
        $right = (static function () use (&$reads): Generator {
            $reads++;

            yield array_to_rows([['name' => 'Alice'], ['name' => 'Bob']], schema(str_schema('name')));
        })();

        static::assertSame(
            [
                [['id' => 1, 'r_name' => 'Alice'], ['id' => 1, 'r_name' => 'Bob']],
                [['id' => 2, 'r_name' => 'Alice'], ['id' => 2, 'r_name' => 'Bob']],
                [['id' => 3, 'r_name' => 'Alice'], ['id' => 3, 'r_name' => 'Bob']],
            ],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    (new CrossJoin('r_'))->join(
                        (static function () {
                            yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                            yield array_to_rows([['id' => 2]], schema(int_schema('id')));
                            yield array_to_rows([['id' => 3]], schema(int_schema('id')));
                        })(),
                        $right,
                        flow_context()->backend(),
                    ),
                    false,
                ),
            ),
        );
        static::assertSame(1, $reads);
    }

    public function test_an_empty_left_never_reads_the_right_side(): void
    {
        $reads = 0;
        $right = (static function () use (&$reads): Generator {
            $reads++;

            yield array_to_rows([['name' => 'Alice']], schema(str_schema('name')));
        })();

        static::assertSame(
            [],
            iterator_to_array(
                (new CrossJoin())->join(
                    (static function () {
                        yield from [];
                    })(),
                    $right,
                    flow_context()->backend(),
                ),
                false,
            ),
        );
        static::assertSame(0, $reads);
    }

    public function test_an_empty_side_yields_no_batch(): void
    {
        static::assertSame(
            [],
            iterator_to_array(
                (new CrossJoin('joined_'))->join(
                    (static function () {
                        yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                    })(),
                    (static function () {
                        yield rows(schema(str_schema('code')));
                    })(),
                    flow_context()->backend(),
                ),
                false,
            ),
        );
        static::assertSame(
            [],
            iterator_to_array(
                (new CrossJoin('joined_'))->join(
                    (static function () {
                        yield rows(schema(int_schema('id')));
                    })(),
                    (static function () {
                        yield array_to_rows([['code' => 'PL']], schema(str_schema('code')));
                    })(),
                    flow_context()->backend(),
                ),
                false,
            ),
        );
    }

    public function test_a_stop_reaches_the_left_side(): void
    {
        $stopped = false;
        $left = (static function () use (&$stopped): Generator {
            $stopped = (yield array_to_rows([['id' => 1]], schema(int_schema('id')))) === Signal::STOP;

            yield array_to_rows([['id' => 2]], schema(int_schema('id')));
        })();
        $joined = (new CrossJoin())->join(
            $left,
            (static function () {
                yield array_to_rows([['name' => 'Alice']], schema(str_schema('name')));
            })(),
            flow_context()->backend(),
        );

        $joined->current();
        $joined->send(Signal::STOP);

        static::assertTrue($stopped);
        static::assertFalse($joined->valid());
    }

    public function test_a_column_on_both_sides_without_a_prefix_is_refused(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage(
            'Column definitions must be unique, duplicated columns: [active], all: [id, country, active, active]',
        );

        iterator_to_array((new CrossJoin())->join(
            (static function () {
                yield array_to_rows(
                    [['id' => 1, 'country' => 'PL', 'active' => false]],
                    schema(int_schema('id'), str_schema('country'), bool_schema('active')),
                );
            })(),
            (static function () {
                yield array_to_rows([['active' => true]], schema(bool_schema('active')));
            })(),
            flow_context()->backend(),
        ));
    }

    public function test_a_prefix_resolves_a_column_on_both_sides(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'country' => 'PL', 'active' => false, '_active' => true],
                ['id' => 2, 'country' => 'PL', 'active' => false, '_active' => true],
            ],
            iterator_to_array(
                (new CrossJoin('_'))->join(
                    (static function () {
                        yield array_to_rows(
                            [
                                ['id' => 1, 'country' => 'PL', 'active' => false],
                                ['id' => 2, 'country' => 'PL', 'active' => false],
                            ],
                            schema(int_schema('id'), str_schema('country'), bool_schema('active')),
                        );
                    })(),
                    (static function () {
                        yield array_to_rows([['active' => true]], schema(bool_schema('active')));
                    })(),
                    flow_context()->backend(),
                ),
                false,
            )[0]->toArray(),
        );
    }
}
