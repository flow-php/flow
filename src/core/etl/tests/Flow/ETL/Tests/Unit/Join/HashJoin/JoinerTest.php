<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\HashJoin\Joiner;
use Flow\ETL\Join\Join;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

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
            $batches(rows(
                row(int_entry('id', 1), str_entry('email', 'alice@flow.php')),
                row(int_entry('id', 2), str_entry('email', 'bob@flow.php')),
            )),
            $batches(rows(
                row(int_entry('user_id', 5), str_entry('contact', 'alice@flow.php'), str_entry('name', 'Alice')),
                row(int_entry('user_id', 2), str_entry('contact', 'other@flow.php'), str_entry('name', 'Bob')),
            )),
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

    public function test_duplicated_right_rows_are_all_joined(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::inner);

        foreach ($joiner->join(
            $batches(rows(row(int_entry('id', 1)))),
            $batches(rows(
                row(int_entry('user_id', 1), str_entry('name', 'Alice')),
                row(int_entry('user_id', 1), str_entry('name', 'Alice')),
            )),
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
            $batches(rows(row(int_entry('id', 1), int_entry('amount', 100)))),
            $batches(rows(
                row(int_entry('user_id', 1), str_entry('role', 'admin')),
                row(int_entry('user_id', 1), str_entry('role', 'writer')),
                row(int_entry('user_id', 2), str_entry('role', 'reader')),
            )),
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
            $batches(rows(row(int_entry('id', 1), int_entry('amount', 100)))),
            $batches(rows(row(int_entry('id', 1), str_entry('name', 'Alice')))),
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

        foreach ($joiner->join(
            $batches(rows(row(int_entry('id', 1)), row(int_entry('id', 2)))),
            $batches(rows(row(str_entry('user_id', '1')), row(int_entry('user_id', 2)))),
        ) as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => 1]], $joined);
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
            $batches(rows(row(int_entry('id', 1), int_entry('amount', 100)))),
            $batches(rows(row(str_entry('user_id', '1'), str_entry('name', 'Alice')))),
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
            $batches(rows(
                row(int_entry('id', 1), int_entry('amount', 100)),
                row(int_entry('id', 404), int_entry('amount', 200)),
            )),
            $batches(rows(row(int_entry('user_id', 1), str_entry('name', 'Alice')))),
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

    public function test_prefix_collision_suggests_different_prefix(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $this->expectException(DuplicatedEntriesException::class);
        $this->expectExceptionMessage('try to use a different join prefix than: "left_"');

        $joiner = new Joiner(join_on(['id' => 'user_id'], join_prefix: 'left_'), Join::inner);

        foreach ($joiner->join(
            $batches(rows(row(int_entry('id', 1), str_entry('left_name', 'collision')))),
            $batches(rows(row(int_entry('user_id', 1), str_entry('name', 'Alice')))),
        ) as $batch) {
            $batch->count();
        }
    }

    public function test_right_join_emits_unmatched_right_rows_with_null_left_entries(): void
    {
        $batches = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joined = [];

        $joiner = new Joiner(join_on(['id' => 'user_id']), Join::right);

        foreach ($joiner->join(
            $batches(rows(row(int_entry('id', 1), int_entry('amount', 100)))),
            $batches(rows(
                row(int_entry('user_id', 1), str_entry('name', 'Alice')),
                row(int_entry('user_id', 2), str_entry('name', 'Bob')),
            )),
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
}
