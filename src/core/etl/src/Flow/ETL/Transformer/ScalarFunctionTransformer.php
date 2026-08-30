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
use function Flow\ETL\DSL\array_to_rows;
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
            $batch = [];

            foreach ($rows as $r) {
                $values = $r->values();

                // @mago-ignore analysis:mixed-assignment
                foreach (type_array()->assert($this->function->eval($r, $context)) as $key => $val) {
                    $values[$this->entryName() . '.' . $key] = $val;
                }

                $batch[] = $values;
            }

            return array_to_rows($batch, $context->hydrator(), $rows->partitions());
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
            return $rows->flatMap($output, static fn(Row $r): array => array_map(
                static fn(mixed $val): Row => new Row([
                    ...$r->values(),
                    $name => $val === null ? null : $definition->type()->cast($val),
                ]),
                // @mago-ignore analysis:mixed-argument
                $function->eval($r, $context),
            ));
        }

        return $rows->map($output, static function (Row $r) use ($function, $definition, $name, $context): Row {
            // @mago-ignore analysis:mixed-assignment
            $value = $function->eval($r, $context);

            return new Row([
                ...$r->values(),
                $name => $value === null ? null : $definition->type()->cast($value),
            ]);
        });
    }

    private function entryName(): string
    {
        return $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;
    }
}
