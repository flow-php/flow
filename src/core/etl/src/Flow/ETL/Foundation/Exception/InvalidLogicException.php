<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Exception;

use function sprintf;

final class InvalidLogicException extends Exception
{
    public static function because(string $format, float|int|string ...$parameters): self
    {
        return new self(sprintf($format, ...$parameters));
    }

    public static function cyclicPlanOnRun(): self
    {
        return self::cyclicPlan('run');
    }

    public static function nestedTransaction(): self
    {
        return self::because('A transaction cannot contain another transaction');
    }

    public static function nodeNotTranslatable(string $kind): self
    {
        return self::because('No physical steps are known for node %s', $kind);
    }

    public static function writeNotOnSpine(string $loader): self
    {
        return self::because('A write root shares no node with the plan: %s', $loader);
    }

    public static function writeRootRewritten(string $given): self
    {
        return self::because('A write root rewrite must return a Write or a Transaction, %s given', $given);
    }

    public static function joinSideIsNotAPlanRoot(string $given): self
    {
        return self::because(
            'The right side of a join must be a frame\'s plan root (Result or Outputs), %s given',
            $given,
        );
    }

    public static function firstConsumerIsATransaction(): self
    {
        return self::because('The first consumer of a plan cannot be a Transaction');
    }

    public static function consumerRewritten(string $given): self
    {
        return self::because(
            'An Outputs consumer rewrite must return a Result, a Write or a Transaction, %s given',
            $given,
        );
    }

    public static function branchFrameNotBuiltFromPrefix(string $branch): self
    {
        return self::because(
            'A branch must return a frame built from the prefix it was given: %s returned another frame. Build on '
            . '$prefix, and call onError() on the frame you write to, not inside the branch',
            $branch,
        );
    }

    private static function cyclicPlan(string $operation): self
    {
        return new self(sprintf(
            'Cannot %s this plan: it reads from a DataFrame that reads back from it. A DataFrame is mutable, '
            . 'so join(), select() and withColumn() can add that edge after both frames exist. Break the cycle '
            . 'by reading the nested frame from its own source.',
            $operation,
        ));
    }
}
