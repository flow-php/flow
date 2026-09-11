<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use function sprintf;

final class InvalidLogicException extends Exception
{
    public static function because(string $format, float|int|string ...$parameters): self
    {
        return new self(sprintf($format, ...$parameters));
    }

    public static function cyclicPlanOnDescribe(): self
    {
        return self::cyclicPlan('describe');
    }

    public static function cyclicPlanOnRun(): self
    {
        return self::cyclicPlan('run');
    }

    private static function cyclicPlan(string $operation): self
    {
        return new self(sprintf(
            'Cannot %s this plan: it reads from a DataFrame that reads back from it. A DataFrame is mutable, '
            . 'so join(), select() and withEntry() can add that edge after both frames exist. Break the cycle '
            . 'by reading the nested frame from its own source.',
            $operation,
        ));
    }
}
