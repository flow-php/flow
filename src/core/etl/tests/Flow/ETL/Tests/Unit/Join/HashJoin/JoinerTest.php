<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Column\AdaptiveBackend;
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

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
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
            new AdaptiveBackend(),
        );

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'email' => 'alice@flow.php'], ['id' => 2, 'email' => 'bob@flow.php']],
                schema(int_schema('id'), str_schema('email')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [
                    ['user_id' => 5, 'contact' => 'alice@flow.php', 'name' => 'Alice'],
                    ['user_id' => 2, 'contact' => 'other@flow.php', 'name' => 'Bob'],
                ],
                schema(int_schema('user_id'), str_schema('contact'), str_schema('name')),
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
            new AdaptiveBackend(),
        );

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [
                    ['id' => 1, 'email' => 'alice@flow.php', 'phone' => '111'],
                    ['id' => 2, 'email' => 'bob@flow.php', 'phone' => '222'],
                ],
                schema(int_schema('id'), str_schema('email'), str_schema('phone')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [
                    ['user_id' => 1, 'contact' => 'alice@flow.php', 'phone' => '999'],
                    ['user_id' => 2, 'contact' => 'other@flow.php', 'phone' => '999'],
                ],
                schema(int_schema('user_id'), str_schema('contact'), str_schema('phone')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::inner, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows([['id' => 1]], schema(int_schema('id'))))),
            JoinSide::of($batches(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice'], ['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::inner, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'amount' => 100]],
                schema(int_schema('id'), int_schema('amount')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [
                    ['user_id' => 1, 'role' => 'admin'],
                    ['user_id' => 1, 'role' => 'writer'],
                    ['user_id' => 2, 'role' => 'reader'],
                ],
                schema(int_schema('user_id'), str_schema('role')),
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

        $joiner = new Joiner(join_on(['id' => 'id']), Join::inner, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'amount' => 100]],
                schema(int_schema('id'), int_schema('amount')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'name' => 'Alice']],
                schema(int_schema('id'), str_schema('name')),
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
        $joiner = new Joiner(Expression::on(new Identical('id', 'user_id')), Join::left_anti, new AdaptiveBackend());

        $right = static function (): Generator {
            yield array_to_rows([['user_id' => '2']], schema(str_schema('user_id')));
            yield array_to_rows([['user_id' => '9']], schema(str_schema('user_id')));
        };

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows([['id' => '1'], ['id' => '2']], schema(str_schema('id'))))),
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
        $joiner = new Joiner(Expression::on(new Identical('id', 'user_id')), Join::left, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'amount' => 100]],
                schema(int_schema('id'), int_schema('amount')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [['user_id' => '1', 'name' => 'Alice']],
                schema(str_schema('user_id'), str_schema('name')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'amount' => 100], ['id' => 404, 'amount' => 200]],
                schema(int_schema('id'), int_schema('amount')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id'], join_prefix: 'left_'), Join::inner, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'left_name' => 'collision']],
                schema(int_schema('id'), str_schema('left_name')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::inner, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'amount' => 100]],
                schema(int_schema('id'), int_schema('amount')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [
                    ['user_id' => 1, 'role' => 'admin'],
                    ['user_id' => 1, 'role' => 'writer'],
                    ['user_id' => 2, 'role' => 'reader'],
                ],
                schema(int_schema('user_id'), str_schema('role')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left_anti, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))))),
            JoinSide::of($batches(array_to_rows([['user_id' => 1]], schema(int_schema('user_id'))))),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'amount' => 100], ['id' => 404, 'amount' => 200]],
                schema(int_schema('id'), int_schema('amount')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'amount' => 100]],
                schema(int_schema('id'), int_schema('amount')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::right, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'amount' => 100]],
                schema(int_schema('id'), int_schema('amount')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice'], ['user_id' => 2, 'name' => 'Bob']],
                schema(int_schema('user_id'), str_schema('name')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::right, new AdaptiveBackend());

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'amount' => 100]],
                schema(int_schema('id'), int_schema('amount')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice'], ['user_id' => 2, 'name' => 'Bob']],
                schema(int_schema('user_id'), str_schema('name')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left, new AdaptiveBackend());

        $emitted = iterator_to_array(
            $joiner->join(
                JoinSide::of($batches(array_to_rows(
                    [['id' => 1, 'amount' => 100], ['id' => 404, 'amount' => 200]],
                    schema(int_schema('id'), int_schema('amount')),
                ))),
                JoinSide::of($batches(array_to_rows(
                    [['user_id' => 1, 'name' => 'Alice']],
                    schema(int_schema('user_id'), str_schema('name')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::right, new AdaptiveBackend());

        $emitted = iterator_to_array(
            $joiner->join(
                JoinSide::of($batches(array_to_rows(
                    [['id' => 1, 'amount' => 100]],
                    schema(int_schema('id'), int_schema('amount')),
                ))),
                JoinSide::of($batches(array_to_rows(
                    [['user_id' => 1, 'name' => 'Alice'], ['user_id' => 404, 'name' => 'Bob']],
                    schema(int_schema('user_id'), str_schema('name')),
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
        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left, new AdaptiveBackend());

        $emitted = iterator_to_array(
            $joiner->join(
                JoinSide::of(
                    (static function (): Generator {
                        yield array_to_rows(
                            [['id' => 1, 'amount' => 100]],
                            schema(int_schema('id'), int_schema('amount')),
                        );
                        yield array_to_rows(
                            [['id' => 404, 'amount' => 200]],
                            schema(int_schema('id'), int_schema('amount')),
                        );
                    })(),
                ),
                JoinSide::of(
                    (static function (): Generator {
                        yield array_to_rows(
                            [['user_id' => 1, 'name' => 'Alice']],
                            schema(int_schema('user_id'), str_schema('name')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left, new AdaptiveBackend());

        $joined = [];

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows([['id' => 1], ['id' => 404]], schema(int_schema('id'))))),
            JoinSide::of($batches(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name'), str_schema('nickname', nullable: true)),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left, new AdaptiveBackend());

        $joined = [];

        foreach ($joiner->join(
            JoinSide::of($batches(array_to_rows(
                [['id' => 1, 'l' => 'has-key'], ['l' => 'no-key']],
                schema(int_schema('id', nullable: true), str_schema('l')),
            ))),
            JoinSide::of($batches(array_to_rows(
                [['user_id' => 1, 'r' => 'Alice']],
                schema(int_schema('user_id'), str_schema('r')),
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

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::left, new AdaptiveBackend());

        $joined = [];

        foreach ($joiner->join(
            JoinSide::of(
                $batches(array_to_rows(
                    [['id' => 404, 'extra' => 'drop-me']],
                    schema(int_schema('id'), str_schema('extra')),
                )),
                null,
                schema(int_schema('id')),
            ),
            JoinSide::of($batches(array_to_rows([['user_id' => 1]], schema(int_schema('user_id'))))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => 404, 'user_id' => null]], $joined);
    }

    public function test_a_build_side_spanning_batches_is_joined_as_one(): void
    {
        $right = static function (): Generator {
            yield array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
            );
            yield array_to_rows([], schema(int_schema('user_id'), str_schema('name')));
            yield array_to_rows([['user_id' => 2, 'name' => 'Bob']], schema(int_schema('user_id'), str_schema('name')));
        };
        $left = static function (): Generator {
            yield array_to_rows([['id' => 2], ['id' => 1], ['id' => 3]], schema(int_schema('id')));
        };

        $joined = [];

        foreach ((new Joiner(join_on(['id' => 'user_id']), Join::left, new AdaptiveBackend()))->join(
            JoinSide::of($left()),
            JoinSide::of($right()),
        ) as $batch) {
            $joined = [...$joined, ...$batch->toArray()];
        }

        static::assertSame(
            [
                ['id' => 2, 'user_id' => 2, 'name' => 'Bob'],
                ['id' => 1, 'user_id' => 1, 'name' => 'Alice'],
                ['id' => 3, 'user_id' => null, 'name' => null],
            ],
            $joined,
        );
    }

    public function test_swapped_unmatched_build_rows_are_emitted_in_chunks_of_batch_size(): void
    {
        $left = static function (): Generator {
            yield array_to_rows([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ], schema(int_schema('id')));
        };
        $right = static function (): Generator {
            yield array_to_rows([['user_id' => 3]], schema(int_schema('user_id')));
        };

        $batches = [];

        foreach ((new Joiner(join_on(['id' => 'user_id']), Join::left, new AdaptiveBackend(), batchSize: 2))->join(
            JoinSide::of($left()),
            JoinSide::of($right()),
            buildLeft: true,
        ) as $batch) {
            $batches[] = $batch->column('id')->values();
        }

        static::assertSame([[3], [1, 2], [4, 5]], $batches);
    }

    public function test_unmatched_build_rows_are_emitted_in_chunks_of_batch_size(): void
    {
        $left = static function (): Generator {
            yield array_to_rows([['id' => 3]], schema(int_schema('id')));
        };
        $right = static function (): Generator {
            yield array_to_rows([
                ['user_id' => 1],
                ['user_id' => 2],
                ['user_id' => 3],
                ['user_id' => 4],
                ['user_id' => 5],
            ], schema(int_schema('user_id')));
        };

        $batches = [];

        foreach ((new Joiner(join_on(['id' => 'user_id']), Join::right, new AdaptiveBackend(), batchSize: 2))->join(
            JoinSide::of($left()),
            JoinSide::of($right()),
        ) as $batch) {
            $batches[] = $batch->toArray();
        }

        static::assertSame(
            [
                [['id' => 3, 'user_id' => 3]],
                [['id' => null, 'user_id' => 1], ['id' => null, 'user_id' => 2]],
                [['id' => null, 'user_id' => 4], ['id' => null, 'user_id' => 5]],
            ],
            $batches,
        );
    }
}
