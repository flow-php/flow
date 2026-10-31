<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Exception\ConstraintViolationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\Constraint;
use Flow\ETL\Operators\Processor\ConstrainedProcessor;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\constraint_sorted_by;
use function Flow\ETL\DSL\constraint_unique;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function is_int;

final class ConstrainedProcessorTest extends FlowTestCase
{
    public function test_constrained_processor_forwards_stop_to_its_upstream(): void
    {
        $upstream = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(5)))->withBatchSize(1);
        $processed = (new ConstrainedProcessor([]))->process(
            FlowTestCase::extracted($upstream, flow_context()->backend(), new Criteria()),
            flow_context()->backend(),
        );

        static::assertTrue($processed->valid());

        $processed->send(Signal::STOP);

        static::assertFalse($processed->valid());
        static::assertSame(1, $upstream->batchesYielded);
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'));

        static::assertEquals($input, (new ConstrainedProcessor([]))->bind($input)->output);
    }

    public function test_handles_empty_constraints(): void
    {
        static::assertCount(
            1,
            iterator_to_array((new ConstrainedProcessor([]))->process(
                (static function () {
                    yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            )),
        );
    }

    public function test_handles_empty_input(): void
    {
        static::assertCount(
            0,
            iterator_to_array((new ConstrainedProcessor([new class implements Constraint {
                public function firstViolation(Rows $rows): ?int
                {
                    return null;
                }

                public function toString(): string
                {
                    return 'always';
                }

                public function violation(Rows $rows, int $index): string
                {
                    return '';
                }
            }]))->process(
                (static function () {
                    yield from [];
                })(),
                flow_context()->backend(),
            )),
        );
    }

    public function test_passes_rows_when_all_constraints_satisfied(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new ConstrainedProcessor([new class implements Constraint {
            public function firstViolation(Rows $rows): ?int
            {
                // @mago-ignore analysis:mixed-assignment
                foreach ($rows->column('id')->values() as $i => $id) {
                    if (!is_int($id) || $id <= 0) {
                        return $i;
                    }
                }

                return null;
            }

            public function toString(): string
            {
                return 'id > 0';
            }

            public function violation(Rows $rows, int $index): string
            {
                return 'id must be greater than 0';
            }
        }]))->process(
            (static function () {
                yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));
        static::assertCount(1, $result);
        static::assertCount(2, $result[0]);
    }

    public function test_throws_exception_for_invalid_constraint_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pipeline constraints must be of type Flow\ETL\Operators\Constraint');
        // @mago-ignore analysis:invalid-argument
        new ConstrainedProcessor(['not a constraint']);
    }

    public function test_throws_exception_when_constraint_violated(): void
    {
        $processor = new ConstrainedProcessor([new class implements Constraint {
            public function firstViolation(Rows $rows): ?int
            {
                // @mago-ignore analysis:mixed-assignment
                foreach ($rows->column('id')->values() as $i => $id) {
                    if (!is_int($id) || $id <= 0) {
                        return $i;
                    }
                }

                return null;
            }

            public function toString(): string
            {
                return 'id > 0';
            }

            public function violation(Rows $rows, int $index): string
            {
                return 'id must be greater than 0';
            }
        }]);
        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => -1]], schema(int_schema('id')));
        })();
        $this->expectException(ConstraintViolationException::class);
        iterator_to_array($processor->process($generator, flow_context()->backend()));
    }

    public function test_the_earliest_violation_wins_and_names_the_stream_row(): void
    {
        $processor = new ConstrainedProcessor([
            constraint_unique('id'),
            constraint_sorted_by(ref('n')),
        ]);
        $generator = (static function () {
            yield array_to_rows(
                [['id' => 1, 'n' => 1], ['id' => 2, 'n' => 2]],
                schema(int_schema('id'), int_schema('n')),
            );
            yield array_to_rows(
                [['id' => 3, 'n' => 3], ['id' => 4, 'n' => 0], ['id' => 3, 'n' => 5]],
                schema(int_schema('id'), int_schema('n')),
            );
        })();

        try {
            iterator_to_array($processor->process($generator, flow_context()->backend()));
            static::fail('expected a ConstraintViolationException');
        } catch (ConstraintViolationException $e) {
            static::assertStringContainsString('Sorted constraint on [n ASC]', $e->getMessage());
            static::assertStringContainsString('current: 0, previous: 3', $e->getMessage());
            static::assertStringEndsWith('in row: 3', $e->getMessage());
        }
    }
}
