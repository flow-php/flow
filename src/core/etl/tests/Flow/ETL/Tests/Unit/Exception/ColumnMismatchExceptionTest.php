<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Exception;

use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\str_schema;
use function str_repeat;

final class ColumnMismatchExceptionTest extends FlowTestCase
{
    /**
     * @return \Generator<string, array{mixed, string}>
     */
    public static function provide_rendered_values(): Generator
    {
        yield 'short string' => ['AB-01', "could not convert 'AB-01' (string) to integer"];
        yield 'long string' => [
            str_repeat('x', 40),
            "could not convert '" . str_repeat('x', 32) . "...' (string) to integer",
        ];
        yield 'float' => [1.5, 'could not convert 1.5 (float) to integer'];
        yield 'true' => [true, 'could not convert true (boolean) to integer'];
        yield 'false' => [false, 'could not convert false (boolean) to integer'];
        yield 'array' => [[1, 2], 'could not convert array (list<integer>) to integer'];
    }

    public function test_a_message_carries_no_row_coordinate(): void
    {
        static::assertSame(
            'Row does not match its schema: column "extra" is not declared by the schema',
            ColumnMismatchException::unexpectedColumn('extra')->getMessage(),
        );
    }

    #[DataProvider('provide_rendered_values')]
    public function test_how_a_value_is_rendered_in_the_message(mixed $value, string $expected): void
    {
        static::assertStringContainsString(
            $expected,
            ColumnMismatchException::valueDoesNotMatch(int_schema('id'), $value)->getMessage(),
        );
    }

    public function test_missing_column(): void
    {
        $exception = ColumnMismatchException::missingColumn(int_schema('amount'));

        static::assertSame(
            'Row does not match its schema: column "amount" declared by the schema is missing from the row',
            $exception->getMessage(),
        );
    }

    public function test_null_in_a_not_null_column(): void
    {
        $exception = ColumnMismatchException::valueDoesNotMatch(str_schema('code'), null);

        static::assertSame(
            'Row does not match its schema: column "code": could not convert null to string, '
            . 'column is not nullable',
            $exception->getMessage(),
        );
    }

    public function test_unexpected_column(): void
    {
        $exception = ColumnMismatchException::unexpectedColumn('extra');

        static::assertSame(
            'Row does not match its schema: column "extra" is not declared by the schema',
            $exception->getMessage(),
        );
    }

    public function test_value_does_not_match(): void
    {
        $exception = ColumnMismatchException::valueDoesNotMatch(str_schema('code'), 1000);

        static::assertSame(
            'Row does not match its schema: column "code": could not convert 1000 (integer) to string',
            $exception->getMessage(),
        );
    }
}
