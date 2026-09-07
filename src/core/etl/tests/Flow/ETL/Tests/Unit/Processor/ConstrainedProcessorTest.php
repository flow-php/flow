<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Constraint;
use Flow\ETL\Exception\ConstraintViolationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Processor\ConstrainedProcessor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class ConstrainedProcessorTest extends FlowTestCase
{
    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'));

        static::assertEquals($input, (new ConstrainedProcessor([]))->bind($input)->output);
    }

    public function test_handles_empty_constraints(): void
    {
        $processor = new ConstrainedProcessor([]);
        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]));
        })();
        $result = iterator_to_array($processor->process($generator, flow_context()));
        static::assertCount(1, $result);
    }

    public function test_handles_empty_input(): void
    {
        $constraint = new class implements Constraint {
            public function isSatisfiedBy(Row $row, Schema $schema): bool
            {
                return true;
            }

            public function toString(): string
            {
                return 'always';
            }

            public function violation(Row $row, Schema $schema): string
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
            public function isSatisfiedBy(Row $row, Schema $schema): bool
            {
                // @mago-ignore analysis:possibly-null-operand,possibly-invalid-operand
                return $row->get('id') > 0;
            }

            public function toString(): string
            {
                return 'id > 0';
            }

            public function violation(Row $row, Schema $schema): string
            {
                return 'id must be greater than 0';
            }
        };
        $processor = new ConstrainedProcessor([$constraint]);
        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        })();
        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        static::assertCount(1, $result);
        static::assertCount(2, $result[0]);
    }

    public function test_throws_exception_for_invalid_constraint_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pipeline constraints must be of type Flow\ETL\Constraint');
        // @mago-ignore analysis:invalid-argument
        new ConstrainedProcessor(['not a constraint']);
    }

    public function test_throws_exception_when_constraint_violated(): void
    {
        $constraint = new class implements Constraint {
            public function isSatisfiedBy(Row $row, Schema $schema): bool
            {
                // @mago-ignore analysis:possibly-null-operand,possibly-invalid-operand
                return $row->get('id') > 0;
            }

            public function toString(): string
            {
                return 'id > 0';
            }

            public function violation(Row $row, Schema $schema): string
            {
                return 'id must be greater than 0';
            }
        };
        $processor = new ConstrainedProcessor([$constraint]);
        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => -1]));
        })();
        $this->expectException(ConstraintViolationException::class);
        iterator_to_array($processor->process($generator, flow_context()));
    }
}
