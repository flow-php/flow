<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Throwable;

final readonly class JoinEachRowsTransformer implements Transformer
{
    private function __construct(
        private DataFrameFactory $factory,
        private Expression $expression,
        private Join $type,
    ) {}

    public static function inner(DataFrameFactory $right, Expression $condition): self
    {
        return new self($right, $condition, Join::inner);
    }

    public static function left(DataFrameFactory $right, Expression $condition): self
    {
        return new self($right, $condition, Join::left);
    }

    public static function leftAnti(DataFrameFactory $right, Expression $condition): self
    {
        return new self($right, $condition, Join::left_anti);
    }

    public static function right(DataFrameFactory $right, Expression $condition): self
    {
        return new self($right, $condition, Join::right);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this, [
            TelemetryAttributes::ATTR_JOIN_TYPE => $this->type->value,
        ]);

        try {
            $rightRows = $this->factory->from($rows)->fetch();

            $result = match ($this->type) {
                Join::left => $rows->joinLeft($rightRows, $this->expression, $context->entryFactory()),
                Join::left_anti => $rows->joinLeftAnti($rightRows, $this->expression),
                Join::right => $rows->joinRight($rightRows, $this->expression, $context->entryFactory()),
                default => $rows->joinInner($rightRows, $this->expression),
            };

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $result->count(),
            ]);

            return $result;
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
