<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Throwable;

use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_equals;
use function sprintf;

final readonly class ScalarFunctionFilterTransformer implements Transformer
{
    public function __construct(
        public ScalarFunction $function,
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            // An empty batch has no schema to bind against.
            if (!$rows->count()) {
                $context->telemetry()->transformationCompleted($this, [
                    TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => 0,
                    TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => 0,
                ]);

                return $rows;
            }

            $schema = $rows->schema();
            $resolver = new ReferenceResolver();
            $function = $resolver->resolve($this->function, $schema);
            $resolver->assertResolved($function, $schema);

            // type_bare() keeps the gate blind to nullability - a null-propagating predicate declares
            // ?boolean, and (bool) null === false is how a NULL row is dropped.
            if (!type_equals(type_bare($function->returns()), type_boolean())) {
                throw new InvalidArgumentException(sprintf(
                    'filter() requires a predicate returning boolean, "%s" returns "%s". '
                    . 'Use an explicit comparison, e.g. ->notEquals(lit(0)).',
                    $function::class,
                    $function->returns()->toString(),
                ));
            }

            $kept = [];

            foreach ($rows->all() as $r) {
                // @mago-ignore analysis:mixed-operand
                if ((bool) $function->eval($r, $context)) {
                    $kept[] = $r;
                }
            }

            $result = Rows::trusted($rows->schema(), $kept);

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
