<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Constraint;
use Flow\ETL\Exception\ConstraintViolationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Processor\ConstrainedProcessor;
use Flow\ETL\Row;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class ConstrainedProcessorTest extends FlowTestCase
{
    public function test_handles_empty_constraints(): void
    {
        $processor = new ConstrainedProcessor([]);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);
    }

    public function test_handles_empty_input(): void
    {
        $constraint = new class implements Constraint {
            public function isSatisfiedBy(Row $row): bool
            {
                return true;
            }

            public function toString(): string
            {
                return 'always';
            }

            public function violation(Row $row): string
            {
                return '';
            }
        };

        $processor = new ConstrainedProcessor([$constraint]);

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(0, $result);
    }

    public function test_passes_rows_when_all_constraints_satisfied(): void
    {
        $constraint = new class implements Constraint {
            public function isSatisfiedBy(Row $row): bool
            {
                return $row->valueOf('id') > 0;
            }

            public function toString(): string
            {
                return 'id > 0';
            }

            public function violation(Row $row): string
            {
                return 'id must be greater than 0';
            }
        };

        $processor = new ConstrainedProcessor([$constraint]);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);
        static::assertCount(2, $result[0]);
    }

    public function test_throws_exception_for_invalid_constraint_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pipeline constraints must be of type Flow\ETL\Constraint');

        /** @phpstan-ignore-next-line */
        new ConstrainedProcessor(['not a constraint']);
    }

    public function test_throws_exception_when_constraint_violated(): void
    {
        $constraint = new class implements Constraint {
            public function isSatisfiedBy(Row $row): bool
            {
                return $row->valueOf('id') > 0;
            }

            public function toString(): string
            {
                return 'id > 0';
            }

            public function violation(Row $row): string
            {
                return 'id must be greater than 0';
            }
        };

        $processor = new ConstrainedProcessor([$constraint]);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', -1)));
        })();

        $this->expectException(ConstraintViolationException::class);

        iterator_to_array($processor->process($generator, flow_context()));
    }
}
