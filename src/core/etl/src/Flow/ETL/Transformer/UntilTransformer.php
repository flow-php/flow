<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;
use Throwable;

use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_equals;
use function sprintf;

final class UntilTransformer implements Transformer
{
    private bool $limitReached = false;

    /**
     * @param null|ScalarFunction $resolved the predicate resolved against the bound schema; only bind() sets it
     */
    public function __construct(
        private readonly ScalarFunction $function,
        private ?ScalarFunction $resolved = null,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->function, $this->resolve($input)), $input);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            if ($this->limitReached) {
                $context->telemetry()->transformationCompleted($this, [
                    TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                    TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => 0,
                ]);

                throw new LimitReachedException(0);
            }

            // the unbound path has no plan to hold the resolved predicate, so it is memoised here -
            // until() stops the whole stream, not one batch
            $this->resolved ??= $this->resolve($rows->schema());
            $nextRows = [];

            foreach ($rows as $row) {
                if (!$this->resolved->eval($row, $context)) {
                    $this->limitReached = true;
                } else {
                    $nextRows[] = $row;
                }
            }

            $result = new Rows($rows->schema(), ...$nextRows);

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $result->count(),
            ]);

            return $result;
        } catch (LimitReachedException $e) {
            throw $e;
        } catch (Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }

    /**
     * @throws InvalidArgumentException
     */
    private function resolve(Schema $input): ScalarFunction
    {
        $resolver = new ReferenceResolver();
        $resolved = $resolver->resolve($this->function, $input);
        $resolver->assertResolved($resolved, $input);

        // type_bare() keeps the gate blind to nullability - a null-propagating predicate declares
        // ?boolean, and an evaluated null stops the stream like false does.
        if (!type_equals(type_bare($resolved->returns()), type_boolean())) {
            throw new InvalidArgumentException(sprintf(
                'until() requires a predicate returning boolean, "%s" returns "%s". '
                . 'Use an explicit comparison, e.g. ->notEquals(lit(0)).',
                $resolved::class,
                $resolved->returns()->toString(),
            ));
        }

        return $resolved;
    }
}
