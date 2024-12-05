<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\{DefinitionConverter, NullLevel};
use Flow\Parquet\ParquetFile\Schema\{Repetition, Repetitions};
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DefinitionConverterTest extends TestCase
{
    private ?DefinitionConverter $definitionConverter = null;

    #[TestWith([0, null, new NullLevel(0)])]
    #[TestWith([1, null, new NullLevel(1), 'Value cannot be null for level "1" and max definition level "1"'])]
    #[TestWith([1, 'value', 'value'])]
    public function test_optional(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::OPTIONAL);

        self::assertSame(1, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value)
            );
        }
    }

    #[TestWith([0, null, new NullLevel(0)])]
    #[TestWith([1, null, new NullLevel(1)])]
    #[TestWith([1, 'value', [], 'Value cannot be not null for level "1" and max definition level "2"'])]
    #[TestWith([2, 'value', 'value'])]
    #[TestWith([3, 'value', [], 'Given definition level "3"  is greater than max level, "2"'])]
    public function test_optional_optional(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::OPTIONAL, Repetition::OPTIONAL);

        self::assertSame(2, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $output = $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value),
                'Expected ' . \json_encode($result) . ' got ' . \json_encode($output)
            );
        }
    }

    #[TestWith([0, null, new NullLevel(0)])]
    #[TestWith([1, null, []])]
    #[TestWith([2, null, [new NullLevel(2)]])]
    #[TestWith([3, 'value', ['value']])]
    public function test_optional_repeated_optional(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::OPTIONAL, Repetition::REPEATED, Repetition::OPTIONAL);

        self::assertSame(3, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value)
            );
        }
    }

    #[TestWith([0, null, new NullLevel(0)])]
    #[TestWith([1, null, []])]
    #[TestWith([2, null, [new NullLevel(2)]])]
    #[TestWith([3, null, [[]]])]
    #[TestWith([4, null, [[new NullLevel(4)]]])]
    #[TestWith([5, 'value', [['value']]])]
    public function test_optional_repeated_optional_repeated_optional(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::OPTIONAL, Repetition::REPEATED, Repetition::OPTIONAL, Repetition::REPEATED, Repetition::OPTIONAL);

        self::assertSame(5, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value)
            );
        }
    }

    #[TestWith([0, null, new NullLevel(0)])]
    #[TestWith([1, null, []])]
    #[TestWith([2, null, [new NullLevel(2)]])]
    #[TestWith([3, null, [[]]])]
    #[TestWith([4, null, [[new NullLevel(4)]]])]
    #[TestWith([5, null, [[[]]]])]
    #[TestWith([6, null, [[[new NullLevel(6)]]]])]
    #[TestWith([7, 1, [[[1]]]])]
    public function test_optional_repeated_optional_repeated_optional_repeated_optional(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::OPTIONAL, Repetition::REPEATED, Repetition::OPTIONAL, Repetition::REPEATED, Repetition::OPTIONAL, Repetition::REPEATED, Repetition::OPTIONAL);

        self::assertSame(7, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value)
            );
        }
    }

    #[TestWith([0, null, new NullLevel(0)])]
    #[TestWith([1, null, []])]
    #[TestWith([2, null, new NullLevel(2), 'Value cannot be null for level "2" and max definition level "2"'])]
    #[TestWith([2, 'value', ['value']])]
    public function test_optional_repeated_required(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::OPTIONAL, Repetition::REPEATED, Repetition::REQUIRED);

        self::assertSame(2, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value)
            );
        }
    }

    #[TestWith([0, null, new NullLevel(0)])]
    #[TestWith([1, 'value', 'value'])]
    #[TestWith([1, null, [], 'Value cannot be null for level "1" and max definition level "1"'])]
    public function test_optional_required(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::OPTIONAL, Repetition::REQUIRED);

        self::assertSame(1, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value)
            );
        }
    }

    #[TestWith([0, null, null, 'Value cannot be null for level "0" and max definition level "0"'])]
    #[TestWith([0, 'value', 'value'])]
    #[TestWith([1, null, null, 'Given definition level "1"  is greater than max level, "0"'])]
    public function test_required(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::REQUIRED);

        self::assertSame(0, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value)
            );
        }
    }

    #[TestWith([0, null, new NullLevel(0)])]
    #[TestWith([1, 'value', 'value'])]
    public function test_required_optional(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::REQUIRED, Repetition::OPTIONAL);

        self::assertSame(1, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value)
            );
        }
    }

    #[TestWith([0, null, []])]
    #[TestWith([1, null, [[]]])]
    #[TestWith([2, 1, [[1]]])]
    public function test_required_repeated_required_repeated_required(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::REQUIRED, Repetition::REPEATED, Repetition::REQUIRED, Repetition::REPEATED, Repetition::REQUIRED);

        self::assertSame(2, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value)
            );
        }
    }

    #[TestWith([0, null, null, 'Value cannot be null for level "0" and max definition level "0"'])]
    #[TestWith([0, 'value', 'value'])]
    public function test_required_required(int $level, mixed $value, mixed $result, ?string $exceptionMessage = null) : void
    {
        $repetitions = new Repetitions(Repetition::REQUIRED, Repetition::REQUIRED);

        self::assertSame(0, $repetitions->maxDefinitionLevel());

        if ($exceptionMessage) {
            $this->expectExceptionMessage($exceptionMessage);
            $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value);
        } else {
            self::assertEquals(
                $result,
                $this->definitionConverter()->toValue($repetitions, definitionLevel: $level, value: $value)
            );
        }
    }

    private function definitionConverter() : DefinitionConverter
    {
        if ($this->definitionConverter) {
            return $this->definitionConverter;
        }
        $this->definitionConverter = new DefinitionConverter();

        return $this->definitionConverter;
    }
}
