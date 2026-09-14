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

    public static function cyclicPlanOnRun(): self
    {
        return self::cyclicPlan('run');
    }

    public static function nestedTransaction(): self
    {
        return self::because('A transaction cannot contain another transaction');
    }

    public static function nodeNotLowerable(string $kind): self
    {
        return self::because('No Lowering registered for node %s', $kind);
    }

    public static function pipelineWithoutSource(string $what): self
    {
        return self::because('%s has no source extractor', $what);
    }

    public static function sinkNotOnSpine(string $sink): self
    {
        return self::because('A sink root shares no node with the plan: %s', $sink);
    }

    public static function sinkRootRewritten(string $given): self
    {
        return self::because('A sink root rewrite must return a Write or a Transaction, %s given', $given);
    }

    public static function resultRewritten(string $given): self
    {
        return self::because('The first child of a SinkMultiple must stay a Result, %s given', $given);
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
