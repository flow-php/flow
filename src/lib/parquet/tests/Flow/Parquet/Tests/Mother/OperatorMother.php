<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Operator;

final class OperatorMother
{
    public static function named(string $name): Operator
    {
        foreach (Operator::cases() as $operator) {
            if ($operator->name === $name) {
                return $operator;
            }
        }

        throw new InvalidArgumentException('No operator ' . $name);
    }
}
