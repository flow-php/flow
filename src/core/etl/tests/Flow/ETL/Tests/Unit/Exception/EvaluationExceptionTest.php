<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Exception;

use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

final class EvaluationExceptionTest extends TestCase
{
    public function test_at_re_indexes_an_evaluation_exception_without_nesting(): void
    {
        $cause = new RuntimeException('division by zero');

        $exception = EvaluationException::at(5, new EvaluationException(1, $cause));

        static::assertSame(5, $exception->rowIndex);
        static::assertSame($cause, $exception->getPrevious());
        static::assertSame('division by zero (row 5)', $exception->getMessage());
    }

    public function test_at_wraps_any_other_exception(): void
    {
        $cause = new RuntimeException('division by zero');

        $exception = EvaluationException::at(2, $cause);

        static::assertSame(2, $exception->rowIndex);
        static::assertSame($cause, $exception->getPrevious());
    }

    public function test_carries_the_row_and_the_cause(): void
    {
        $cause = new RuntimeException('division by zero');

        $exception = new EvaluationException(3, $cause);

        static::assertSame(3, $exception->rowIndex);
        static::assertSame($cause, $exception->getPrevious());
        static::assertSame('division by zero (row 3)', $exception->getMessage());
    }
}
