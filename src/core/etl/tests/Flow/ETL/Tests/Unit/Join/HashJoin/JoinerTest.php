<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\HashJoin\Joiner;
use Flow\ETL\Join\HashJoin\JoinSide;
use Flow\ETL\Join\Join;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JoinerTest extends FlowTestCase
{
    public function test_any_comparison_falls_back_to_verifying_every_pair(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(
            Expression::on(new Any(new Equal('id', 'user_id'), new Equal('email', 'contact'))),
            Join::inner,
        );

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), str_schema('email')),
                row(['id' => 1, 'email' => 'alice@flow.php']),
                row(['id' => 2, 'email' => 'bob@flow.php']),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('contact'), str_schema('name')),
                row(['user_id' => 5, 'contact' => 'alice@flow.php', 'name' => 'Alice']),
                row(['user_id' => 2, 'contact' => 'other@flow.php', 'name' => 'Bob']),
            ))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                [
                    'id' => 1,
                    'email' => 'alice@flow.php',
                    'user_id' => 5,
                    'contact' => 'alice@flow.php',
                    'name' => 'Alice',
                ],
                ['id' => 2, 'email' => 'bob@flow.php', 'user_id' => 2, 'contact' => 'other@flow.php', 'name' => 'Bob'],
            ],
            $joined,
        );
    }

    public function test_all_with_any_hashes_by_the_equality_and_verifies_the_rest(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(
            Expression::on(
                new All(
                    new Equal('id', 'user_id'),
                    new Any(new Equal('email', 'contact'), new Equal('phone', 'phone')),
                ),
            ),
            Join::inner,
        );

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), str_schema('email'), str_schema('phone')),
                row(['id' => 1, 'email' => 'alice@flow.php', 'phone' => '111']),
                row(['id' => 2, 'email' => 'bob@flow.php', 'phone' => '222']),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('contact'), str_schema('phone')),
                row(['user_id' => 1, 'contact' => 'alice@flow.php', 'phone' => '999']),
                row(['user_id' => 2, 'contact' => 'other@flow.php', 'phone' => '999']),
            ))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        // id=1 matches via email, id=2 has an id match but fails the Any residual
        static::assertSame(
            [
                [
                    'id' => 1,
                    'email' => 'alice@flow.php',
                    'phone' => '111',
                    'user_id' => 1,
                    'contact' => 'alice@flow.php',
                ],
            ],
            $joined,
        );
    }

    public function test_duplicated_right_rows_are_all_joined(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::inner);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(schema(int_schema('id')), row(['id' => 1])))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertCount(2, $joined);
    }

    public function test_inner_join_emits_every_matching_right_row(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::inner);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('role')),
                row(['user_id' => 1, 'role' => 'admin']),
                row(['user_id' => 1, 'role' => 'writer']),
                row(['user_id' => 2, 'role' => 'reader']),
            ))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'role' => 'admin'],
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'role' => 'writer'],
            ],
            $joined,
        );
    }

    public function test_inner_join_drops_duplicated_right_join_columns(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'id']), Join::inner);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 1, 'name' => 'Alice']),
            ))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => 1, 'amount' => 100, 'name' => 'Alice']], $joined);
    }

    public function test_left_anti_join_keeps_left_row_on_hash_hit_without_match(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        // int 1 and string "1" hash into the same bucket but are not identical
        $joiner = new Joiner(Expression::on(new Identical('id', 'user_id')), Join::left_anti);

        $right = static function (): Generator {
            yield rows(schema(str_schema('user_id')), row(['user_id' => '2']));
            yield rows(schema(str_schema('user_id')), row(['user_id' => '9']));
        };

        foreach ($joiner->join(
            JoinSide::of($batches(rows(schema(str_schema('id')), row(['id' => '1']), row(['id' => '2'])))),
            JoinSide::of($right()),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        // '2' is identical to the right's '2' and drops out; '1' only collides by hash with int 1
        static::assertSame([['id' => '1']], $joined);
    }

    public function test_left_join_keeps_left_row_on_hash_hit_without_match(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        // int 1 and string "1" hash into the same bucket but are not identical
        $joiner = new Joiner(Expression::on(new Identical('id', 'user_id')), Join::left);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
            ))),
            JoinSide::of($batches(rows(
                schema(str_schema('user_id'), str_schema('name')),
                row(['user_id' => '1', 'name' => 'Alice']),
            ))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => 1, 'amount' => 100, 'user_id' => null, 'name' => null]], $joined);
    }

    public function test_left_join_pads_unmatched_left_rows_with_nulls(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
                row(['id' => 404, 'amount' => 200]),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'name' => 'Alice'],
                ['id' => 404, 'amount' => 200, 'user_id' => null, 'name' => null],
            ],
            $joined,
        );
    }

    public function test_prefix_collision_with_left_column_throws(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage('Entry definitions must be unique, duplicated entries: [left_name]');

        $joiner = new Joiner(join_on(['id' => 'user_id'], join_prefix: 'left_'), Join::inner);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), str_schema('left_name')),
                row(['id' => 1, 'left_name' => 'collision']),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
        ) as $batch) {
            $batch->count();
        }
    }

    public function test_swapped_inner_join_emits_the_same_rows(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::inner);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('role')),
                row(['user_id' => 1, 'role' => 'admin']),
                row(['user_id' => 1, 'role' => 'writer']),
                row(['user_id' => 2, 'role' => 'reader']),
            ))),
            buildLeft: true,
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'role' => 'admin'],
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'role' => 'writer'],
            ],
            $joined,
        );
    }

    public function test_swapped_left_anti_join_emits_unmatched_left_rows_at_the_end(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left_anti);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])))),
            JoinSide::of($batches(rows(schema(int_schema('user_id')), row(['user_id' => 1])))),
            buildLeft: true,
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => 2]], $joined);
    }

    public function test_swapped_left_join_pads_unmatched_left_rows_at_the_end(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
                row(['id' => 404, 'amount' => 200]),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
            buildLeft: true,
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'name' => 'Alice'],
                ['id' => 404, 'amount' => 200, 'user_id' => null, 'name' => null],
            ],
            $joined,
        );
    }

    public function test_swapped_left_join_with_empty_right_side_pads_every_left_row(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $empty = static function (): Generator {
            yield from [];
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
            ))),
            JoinSide::of($empty()),
            buildLeft: true,
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => 1, 'amount' => 100]], $joined);
    }

    public function test_swapped_right_join_pads_unmatched_right_rows_inline(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::right);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
                row(['user_id' => 2, 'name' => 'Bob']),
            ))),
            buildLeft: true,
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'name' => 'Alice'],
                ['id' => null, 'amount' => null, 'user_id' => 2, 'name' => 'Bob'],
            ],
            $joined,
        );
    }

    public function test_right_join_emits_unmatched_right_rows_with_null_left_entries(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::right);

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
                row(['user_id' => 2, 'name' => 'Bob']),
            ))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'name' => 'Alice'],
                ['id' => null, 'amount' => null, 'user_id' => 2, 'name' => 'Bob'],
            ],
            $joined,
        );
    }

    public function test_left_join_emits_a_schema_declaring_the_right_side_nullable(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left);

        $emitted = iterator_to_array(
            $joiner->join(
                JoinSide::of($batches(rows(
                    schema(int_schema('id'), int_schema('amount')),
                    row(['id' => 1, 'amount' => 100]),
                    row(['id' => 404, 'amount' => 200]),
                ))),
                JoinSide::of($batches(rows(
                    schema(int_schema('user_id'), str_schema('name')),
                    row(['user_id' => 1, 'name' => 'Alice']),
                ))),
            ),
            preserve_keys: false,
        );

        static::assertEquals(
            schema(
                int_schema('id'),
                int_schema('amount'),
                int_schema('user_id', nullable: true),
                str_schema('name', nullable: true),
            ),
            $emitted[0]->schema(),
        );
    }

    public function test_right_join_emits_a_schema_declaring_the_left_side_nullable(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::right);

        $emitted = iterator_to_array(
            $joiner->join(
                JoinSide::of($batches(rows(
                    schema(int_schema('id'), int_schema('amount')),
                    row(['id' => 1, 'amount' => 100]),
                ))),
                JoinSide::of($batches(rows(
                    schema(int_schema('user_id'), str_schema('name')),
                    row(['user_id' => 1, 'name' => 'Alice']),
                    row(['user_id' => 404, 'name' => 'Bob']),
                ))),
            ),
            preserve_keys: false,
        );

        static::assertEquals(
            schema(
                int_schema('id', nullable: true),
                int_schema('amount', nullable: true),
                int_schema('user_id'),
                str_schema('name'),
            ),
            $emitted[0]->schema(),
        );
    }

    public function test_every_batch_of_a_multi_batch_left_side_shares_one_output_schema(): void
    {
        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left);

        $emitted = iterator_to_array(
            $joiner->join(
                JoinSide::of(
                    (static function (): Generator {
                        yield rows(schema(int_schema('id'), int_schema('amount')), row(['id' => 1, 'amount' => 100]));
                        yield rows(schema(int_schema('id'), int_schema('amount')), row(['id' => 404, 'amount' => 200]));
                    })(),
                ),
                JoinSide::of(
                    (static function (): Generator {
                        yield rows(
                            schema(int_schema('user_id'), str_schema('name')),
                            row(['user_id' => 1, 'name' => 'Alice']),
                        );
                    })(),
                ),
            ),
            preserve_keys: false,
        );

        static::assertCount(2, $emitted);
        static::assertEquals($emitted[0]->schema(), $emitted[1]->schema());
    }

    public function test_a_padded_row_covers_a_right_column_no_source_row_carried(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left);

        $joined = [];

        foreach ($joiner->join(
            JoinSide::of($batches(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 404])))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('name'), str_schema('nickname', nullable: true)),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'user_id' => 1, 'name' => 'Alice', 'nickname' => null],
                ['id' => 404, 'user_id' => null, 'name' => null, 'nickname' => null],
            ],
            $joined,
        );
    }

    /**
     * An unknown key matches nothing in SQL rather than aborting the join: the null keys into a bucket
     * and no comparison in it succeeds, so the row falls out unmatched.
     */
    public function test_a_row_omitting_a_nullable_join_key_is_unmatched_rather_than_refused(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left);

        $joined = [];

        foreach ($joiner->join(
            JoinSide::of($batches(rows(
                schema(int_schema('id', nullable: true), str_schema('l')),
                row(['id' => 1, 'l' => 'has-key']),
                row(['l' => 'no-key']),
            ))),
            JoinSide::of($batches(rows(
                schema(int_schema('user_id'), str_schema('r')),
                row(['user_id' => 1, 'r' => 'Alice']),
            ))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'l' => 'has-key', 'user_id' => 1, 'r' => 'Alice'],
                ['id' => null, 'l' => 'no-key', 'user_id' => null, 'r' => null],
            ],
            $joined,
        );
    }

    public function test_a_left_batch_wider_than_the_output_schema_is_projected_not_refused(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left);

        $joined = [];

        foreach ($joiner->join(
            JoinSide::of(
                $batches(rows(schema(int_schema('id'), str_schema('extra')), row(['id' => 404, 'extra' => 'drop-me']))),
                null,
                schema(int_schema('id')),
            ),
            JoinSide::of($batches(rows(schema(int_schema('user_id')), row(['user_id' => 1])))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => 404, 'user_id' => null]], $joined);
    }
}
