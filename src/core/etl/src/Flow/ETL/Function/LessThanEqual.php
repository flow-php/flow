<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\ValueComparator;

final class LessThanEqual extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $left,
        private readonly mixed $right,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        $leftParam = new Parameter($this->left);
        $rightParam = new Parameter($this->right);

        $leftType = $leftParam->asType($row, $context);
        $rightType = $rightParam->asType($row, $context);

        (new ValueComparator())->assertComparableTypes($leftType, $rightType, '<=');

        if (
            $leftType instanceof IntegerType
            || $leftType instanceof FloatType
            || $rightType instanceof IntegerType
            || $rightType instanceof FloatType
        ) {
            $left = $leftParam->asNumber($row, $context);
            $right = $rightParam->asNumber($row, $context);

            if ($left === null || $right === null) {
                return $context
                    ->functions()
                    ->invalidResult(new InvalidArgumentException('LessThanEqual function requires non-null values'));
            }

            return $left <= $right;
        }

        if ($leftType instanceof StringType || $leftType instanceof JsonType) {
            $left = $leftParam->asString($row, $context);
            $right = $rightParam->asString($row, $context);

            if ($left === null || $right === null) {
                return $context
                    ->functions()
                    ->invalidResult(new InvalidArgumentException('LessThanEqual function requires non-null values'));
            }

            return $left <= $right;
        }

        if ($leftType instanceof DateTimeType || $leftType instanceof DateType) {
            $left = $leftParam->asInstanceOf($row, $context, DateTimeInterface::class);
            $right = $rightParam->asInstanceOf($row, $context, DateTimeInterface::class);

            if ($left === null || $right === null) {
                return $context
                    ->functions()
                    ->invalidResult(new InvalidArgumentException('LessThanEqual function requires non-null values'));
            }

            return $left <= $right;
        }

        if ($leftType instanceof TimeType) {
            $left = $leftParam->asInstanceOf($row, $context, DateInterval::class);
            $right = $rightParam->asInstanceOf($row, $context, DateInterval::class);

            if ($left === null || $right === null) {
                return $context
                    ->functions()
                    ->invalidResult(new InvalidArgumentException('LessThanEqual function requires non-null values'));
            }

            $reference = new DateTimeImmutable('@0');

            return $reference->add($left) <= $reference->add($right);
        }

        $left = $leftParam->asArray($row, $context);
        $right = $rightParam->asArray($row, $context);

        if ($left === null || $right === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('LessThanEqual function requires non-null values'));
        }

        return $left <= $right;
    }
}
