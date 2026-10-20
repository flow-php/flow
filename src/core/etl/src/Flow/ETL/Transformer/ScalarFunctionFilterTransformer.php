<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\BoundStep;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\Filter;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Throwable;

final readonly class ScalarFunctionFilterTransformer implements Transformer
{
    /**
     * @param null|Filter $filter the predicate bound to the input schema; only bind() sets it
     */
    public function __construct(
        public ScalarFunction $function,
        private ?Filter $filter = null,
    ) {}

    /**
     * @throws InvalidArgumentException
     */
    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->function, Filter::bind($input, $this->function)), $input);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $result = ($this->filter ?? Filter::bind($rows->schema(), $this->function))->filter($rows, $context);

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
