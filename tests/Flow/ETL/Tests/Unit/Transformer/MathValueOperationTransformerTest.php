<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\Math\Operation;
use PHPUnit\Framework\TestCase;

final class MathValueOperationTransformerTest extends TestCase
{
    public function math_operations_provider() : \Generator
    {
        yield [new Entry\IntegerEntry('left', 10), 10, Operation::add, 20, Entry\IntegerEntry::class];
        yield [new Entry\IntegerEntry('left', 10), 10, Operation::subtract, 0, Entry\IntegerEntry::class];
        yield [new Entry\IntegerEntry('left', 10), 5, Operation::divide, 2, Entry\IntegerEntry::class];
        yield [new Entry\IntegerEntry('left', 10), 5, Operation::multiply, 50, Entry\IntegerEntry::class];
        yield [new Entry\IntegerEntry('left', 2), 3, Operation::power, 8, Entry\IntegerEntry::class];
        yield [new Entry\IntegerEntry('left', 5), 2, Operation::divide, 2.5, Entry\FloatEntry::class];
        yield [new Entry\IntegerEntry('left', 5), 2, Operation::modulo, 1, Entry\IntegerEntry::class];
    }

    /**
     * @dataProvider math_operations_provider
     */
    public function test_math_operations(Entry $leftEntry, int|float $rightValue, Operation $operation, int|float $result, string $resultClass) : void
    {
        $rows = match ($operation) {
            Operation::add => Transform::add_value($leftEntry->name(), $rightValue)->transform(new Rows(Row::create($leftEntry)), new FlowContext(Config::default())),
            Operation::subtract => Transform::subtract_value($leftEntry->name(), $rightValue)->transform(new Rows(Row::create($leftEntry)), new FlowContext(Config::default())),
            Operation::divide => Transform::divide_by($leftEntry->name(), $rightValue)->transform(new Rows(Row::create($leftEntry)), new FlowContext(Config::default())),
            Operation::multiply => Transform::multiply_by($leftEntry->name(), $rightValue)->transform(new Rows(Row::create($leftEntry)), new FlowContext(Config::default())),
            Operation::modulo => Transform::modulo_by($leftEntry->name(), $rightValue)->transform(new Rows(Row::create($leftEntry)), new FlowContext(Config::default())),
            Operation::power => Transform::power_of($leftEntry->name(), $rightValue)->transform(new Rows(Row::create($leftEntry)), new FlowContext(Config::default())),
        };

        $this->assertSame(
            [
                [
                    'left' => $result,
                ],
            ],
            $rows->toArray()
        );
        $this->assertInstanceOf(
            $resultClass,
            $rows->first()->get($leftEntry->name())
        );
    }
}
