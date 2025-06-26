<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use function Flow\Types\DSL\{type_boolean, type_float, type_integer, type_string};
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type\Comparison\Operator;
use Flow\Types\Type\ValueComparator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ValueComparatorTest extends TestCase
{
    public static function comparable_types_data_provider() : \Generator
    {
        yield [type_integer(), type_integer(), Operator::EQUAL];
        yield [type_integer(), type_float(), Operator::GREATER_THAN];
        yield [type_float(), type_integer(), Operator::LESS_THAN];
        yield [type_string(), type_string(), Operator::IDENTICAL];
        yield [type_integer(), type_string(), Operator::EQUAL];
        yield [type_float(), type_string(), Operator::NOT_EQUAL];
        yield [type_string(), type_integer(), Operator::GREATER_THAN];
        yield [type_string(), type_float(), Operator::LESS_THAN];
    }

    public static function incomparable_types_data_provider() : \Generator
    {
        yield [type_boolean(), type_integer(), Operator::GREATER_THAN];
        yield [type_string(), type_boolean(), Operator::LESS_THAN];
        yield [type_boolean(), type_string(), Operator::EQUAL];
        yield [type_boolean(), type_float(), Operator::NOT_EQUAL];
    }

    public static function operator_string_data_provider() : \Generator
    {
        yield ['==', Operator::EQUAL];
        yield ['!=', Operator::NOT_EQUAL];
        yield ['>', Operator::GREATER_THAN];
        yield ['<', Operator::LESS_THAN];
        yield ['>=', Operator::GREATER_THAN_EQUAL];
        yield ['<=', Operator::LESS_THAN_EQUAL];
        yield ['===', Operator::IDENTICAL];
        yield ['!==', Operator::NOT_IDENTICAL];
        yield ['<>', Operator::DIFFERENT];
        yield ['<=>', Operator::SPACE_SHIP];
    }

    public function test_assert_all_types_comparable_checks_all_combinations() : void
    {
        $comparator = new ValueComparator();
        $types = [type_boolean(), type_integer(), type_string()];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't compare '(integer === boolean)' due to data type mismatch.");

        $comparator->assertAllTypesComparable($types, Operator::IDENTICAL);
    }

    public function test_assert_all_types_comparable_with_compatible_types() : void
    {
        $comparator = new ValueComparator();
        $types = [type_integer(), type_float(), type_integer()];

        $this->expectNotToPerformAssertions();
        $comparator->assertAllTypesComparable($types, Operator::GREATER_THAN);
    }

    public function test_assert_all_types_comparable_with_empty_array() : void
    {
        $comparator = new ValueComparator();
        $types = [];

        $this->expectNotToPerformAssertions();
        $comparator->assertAllTypesComparable($types, Operator::EQUAL);
    }

    public function test_assert_all_types_comparable_with_incompatible_types() : void
    {
        $comparator = new ValueComparator();
        $types = [type_boolean(), type_string(), type_integer()];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't compare '(string == boolean)' due to data type mismatch.");

        $comparator->assertAllTypesComparable($types, Operator::EQUAL);
    }

    public function test_assert_all_types_comparable_with_single_type() : void
    {
        $comparator = new ValueComparator();
        $types = [type_integer()];

        $this->expectNotToPerformAssertions();
        $comparator->assertAllTypesComparable($types, Operator::EQUAL);
    }

    public function test_assert_all_types_comparable_with_string_operator() : void
    {
        $comparator = new ValueComparator();
        $types = [type_integer(), type_float()];

        $this->expectNotToPerformAssertions();
        $comparator->assertAllTypesComparable($types, '>=');
    }

    public function test_assert_all_types_comparable_with_string_operator_incompatible() : void
    {
        $comparator = new ValueComparator();
        $types = [type_boolean(), type_integer()];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't compare '(integer != boolean)' due to data type mismatch.");

        $comparator->assertAllTypesComparable($types, '!=');
    }

    #[DataProvider('comparable_types_data_provider')]
    public function test_assert_comparable_types_with_compatible_types(mixed $left, mixed $right, Operator $operator) : void
    {
        $comparator = new ValueComparator();

        $this->expectNotToPerformAssertions();
        $comparator->assertComparableTypes($left, $right, $operator);
    }

    #[DataProvider('incomparable_types_data_provider')]
    public function test_assert_comparable_types_with_incompatible_types(mixed $left, mixed $right, Operator $operator) : void
    {
        $comparator = new ValueComparator();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(\sprintf("Can't compare '(%s %s %s)' due to data type mismatch.", $left->toString(), $operator->value, $right->toString()));

        $comparator->assertComparableTypes($left, $right, $operator);
    }

    #[DataProvider('operator_string_data_provider')]
    public function test_assert_comparable_types_with_string_operator(string $operatorString, Operator $operator) : void
    {
        $comparator = new ValueComparator();
        $left = type_integer();
        $right = type_integer();

        $this->expectNotToPerformAssertions();
        $comparator->assertComparableTypes($left, $right, $operatorString);
    }

    public function test_assert_comparable_types_with_string_operator_incompatible() : void
    {
        $comparator = new ValueComparator();
        $left = type_boolean();
        $right = type_string();
        $operatorString = '==';

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(\sprintf("Can't compare '(%s %s %s)' due to data type mismatch.", $left->toString(), Operator::EQUAL->value, $right->toString()));

        $comparator->assertComparableTypes($left, $right, $operatorString);
    }
}
