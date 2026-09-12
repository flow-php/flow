<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Transformer;
use Throwable;

use function count;
use function Flow\Types\DSL\type_array;

final readonly class NestedExpandTransformer implements Transformer
{
    /**
     * @param Definition<mixed>|Schema $declares
     */
    public function __construct(
        private ScalarFunctionTransformer $unbound,
        private string $entryName,
        private NestedExpansion $expansion,
        private Definition|Schema $declares,
        private Schema $output,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return $this->unbound->bind($input);
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this, [
            TelemetryAttributes::ATTR_SCALAR_FUNCTION => $this->unbound->function::class,
        ]);

        try {
            $result = $this->declares instanceof Definition
                ? $this->map($rows, $context, $this->declares)
                : $this->unpack($rows, $context, $this->declares);

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

    /**
     * @param Definition<mixed> $derived
     */
    private function map(Rows $rows, FlowContext $context, Definition $derived): Rows
    {
        $columns = new DerivedColumns();
        $declared = $columns->declare($rows->schema(), $derived);
        $name = $derived->entry()->name();
        $mapped = [];

        foreach ($rows->all() as $r) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->expansion->eval($r, $context) as $value) {
                $mapped[] = new Row([...$r->values(), $name => $columns->value($derived, $value, count($mapped))]);
            }
        }

        return $columns->rows($declared, $this->output, $mapped);
    }

    private function unpack(Rows $rows, FlowContext $context, Schema $declared): Rows
    {
        $columns = new UnpackedColumns();
        $unpacked = [];

        foreach ($rows->all() as $r) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->expansion->eval($r, $context) as $payload) {
                $unpacked[] = new Row($columns->values(
                    $r->values(),
                    $this->entryName . '.',
                    $declared,
                    type_array()->assert($payload),
                ));
            }
        }

        return new Rows($this->output, ...$unpacked);
    }
}
