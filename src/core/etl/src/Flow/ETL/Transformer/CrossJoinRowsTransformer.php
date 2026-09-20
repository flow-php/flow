<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\BoundStep;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Executor;
use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\JoinSchema;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Throwable;

final class CrossJoinRowsTransformer implements Transformer
{
    private ?Rows $rows = null;

    public function __construct(
        public readonly PhysicalPlan $right,
        private readonly Executor $executor,
        public readonly string $prefix = '',
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, (new JoinSchema($this->prefix))->cross($input, $this->right->schema()));
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $result = $rows->joinCross($this->rows(), $this->prefix);

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

    private function rows(): Rows
    {
        if ($this->rows === null) {
            $this->rows = $this->executor->merge($this->executor->executePipeline($this->right->root()), $this->right);
        }

        return $this->rows;
    }
}
