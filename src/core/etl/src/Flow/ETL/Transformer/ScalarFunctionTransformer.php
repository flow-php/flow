<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Transformer;
use Throwable;

use function array_map;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_array;

final readonly class ScalarFunctionTransformer implements Transformer
{
    /**
     * @param Definition<mixed>|string $entry
     */
    public function __construct(
        private string|Definition $entry,
        public ScalarFunction $function,
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this, [
            TelemetryAttributes::ATTR_SCALAR_FUNCTION => $this->function::class,
        ]);

        try {
            $result = $this->doTransform($rows, $context);

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

    private function doTransform(Rows $rows, FlowContext $context): Rows
    {
        // An empty batch has no schema to bind against.
        if (!$rows->count()) {
            return $rows;
        }

        // N columns whose names come from runtime array keys cannot be declared before rows flow -
        // ArrayUnpack::returns() throws SchemaNotDerivableException by design.
        if ($this->function instanceof UnpackResults) {
            return $rows->map(function (Row $r) use ($context): Row {
                // @mago-ignore analysis:mixed-assignment
                foreach (type_array()->assert($this->function->eval($r, $context)) as $key => $val) {
                    $r = $r->set($context->entryFactory()->create($this->entryName() . '.' . $key, $val));
                }

                return $r;
            });
        }

        $schema = $rows->schema();
        $resolver = new ReferenceResolver();
        $function = $resolver->resolve($this->function, $schema);
        $resolver->assertResolved($function, $schema);

        $definition = $this->entry instanceof Definition
            ? $this->entry
            : definition_from_type($this->entryName(), $function->returns());

        if ($function instanceof ExpandResults) {
            return $rows->flatMap(static fn(Row $r): array => array_map(
                static fn($val): Row => new Row($r->entries()->set($context->entryFactory()->fromDefinition(
                    $definition,
                    $val === null ? null : $definition->type()->cast($val),
                ))),
                // @mago-ignore analysis:mixed-argument
                $function->eval($r, $context),
            ));
        }

        return $rows->map(static function (Row $r) use ($function, $definition, $context): Row {
            // @mago-ignore analysis:mixed-assignment
            $value = $function->eval($r, $context);

            return $r->set($context->entryFactory()->fromDefinition(
                $definition,
                $value === null ? null : $definition->type()->cast($value),
            ));
        });
    }

    private function entryName(): string
    {
        return $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;
    }
}
