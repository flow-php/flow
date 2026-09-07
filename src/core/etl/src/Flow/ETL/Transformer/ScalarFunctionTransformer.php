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
use Flow\ETL\Row\InferredBatch;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Transformer;
use Throwable;

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
        // ArrayUnpack::returns() throws SchemaNotDerivableException by design. This is the one
        // PERMANENT schemaless producer.
        if ($this->function instanceof UnpackResults) {
            $batch = [];

            foreach ($rows as $r) {
                $values = $r->values();

                // @mago-ignore analysis:mixed-assignment
                foreach (type_array()->assert($this->function->eval($r, $context)) as $key => $val) {
                    $values[$this->entryName() . '.' . $key] = $val;
                }

                $batch[] = new RawRowValues($values);
            }

            return (new InferredBatch())->of($batch);
        }

        $schema = $rows->schema();
        $resolver = new ReferenceResolver();
        $function = $resolver->resolve($this->function, $schema);
        $resolver->assertResolved($function, $schema);

        $definition = $this->entry instanceof Definition
            ? $this->entry
            : definition_from_type($this->entryName(), $function->returns());

        $name = $definition->entry()->name();
        $output = $schema->findDefinition($name) === null
            ? $schema->add($definition)
            : $schema->replace($name, $definition);

        if ($function instanceof ExpandResults) {
            $expanded = [];

            foreach ($rows->all() as $r) {
                // @mago-ignore analysis:mixed-assignment
                foreach (type_array()->assert($function->eval($r, $context)) as $val) {
                    $expanded[] = new Row([
                        ...$r->values(),
                        $name => $val === null ? null : $definition->type()->cast($val),
                    ]);
                }
            }

            return new Rows($output, ...$expanded);
        }

        $mapped = [];

        foreach ($rows->all() as $r) {
            // @mago-ignore analysis:mixed-assignment
            $value = $function->eval($r, $context);

            $mapped[] = new Row([
                ...$r->values(),
                $name => $value === null ? null : $definition->type()->cast($value),
            ]);
        }

        return new Rows($output, ...$mapped);
    }

    private function entryName(): string
    {
        return $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;
    }
}
