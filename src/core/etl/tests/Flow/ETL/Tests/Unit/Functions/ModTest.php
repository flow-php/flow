<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Tests\Context\BackendsContext;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\Backend;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class ModTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{array<string, ?int>, string}>
     */
    public static function refusals(): Generator
    {
        yield 'zero modulus' => [['a' => 5, 'b' => 0], 'Mod function cannot perform modulo by zero (row 0)'];
        yield 'negative modulus' => [['a' => 5, 'b' => -3], 'Modulus must not be negative, got "-3". (row 0)'];
        yield 'null operand' => [['a' => null, 'b' => 3], 'Mod function requires non-null values (row 0)'];
    }

    /**
     * @return Generator<string, list<mixed>>
     */
    public static function refusalsPerBackend(): Generator
    {
        return BackendsContext::perBackend(self::refusals());
    }

    /**
     * @param array<string, ?int> $row
     */
    #[DataProvider('refusalsPerBackend')]
    public function test_a_refusal_message(Backend $backend, array $row, string $message): void
    {
        try {
            (new FunctionContext(flow_context(config_builder()->backend($backend)->build())))->eval(
                ref('a')->mod(ref('b')),
                $row,
                schema(int_schema('a', nullable: true), int_schema('b', nullable: true)),
            );
            static::fail('the operands must be refused');
        } catch (EvaluationException $e) {
            static::assertSame($message, $e->getMessage());
        }
    }

    public function test_a_negative_remainder_folds_back_under_a_positive_modulus(): void
    {
        static::assertSame(
            [2, 1, 0],
            (new FunctionContext(flow_context()))
                ->column(
                    ref('a')->mod(ref('b')),
                    [['a' => -7, 'b' => 3], ['a' => 7, 'b' => 3], ['a' => PHP_INT_MIN, 'b' => 2]],
                    schema(int_schema('a'), int_schema('b')),
                )
                ->values(),
        );
    }
}
