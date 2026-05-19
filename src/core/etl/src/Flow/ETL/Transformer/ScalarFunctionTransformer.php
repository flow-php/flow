<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Transformer;
use Throwable;

use function array_map;
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
            'scalar.function' => $this->function::class,
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
        if ($this->function instanceof ExpandResults) {
            return $rows->flatMap(fn(Row $r): array => array_map(
                fn($val): Row => new Row($r->entries()->set($context->entryFactory()->create(
                    $this->entryName(),
                    $val,
                    $this->entry instanceof Definition ? $this->entry : null,
                ))),
                // @mago-ignore analysis:mixed-argument
                $this->function->eval($r, $context),
            ));
        }

        if ($this->function instanceof UnpackResults) {
            return $rows->map(function (Row $r) use ($context): Row {
                // @mago-ignore analysis:mixed-assignment
                foreach (type_array()->assert($this->function->eval($r, $context)) as $key => $val) {
                    $r = $r->set($context->entryFactory()->create($this->entryName() . '.' . $key, $val));
                }

                return $r;
            });
        }

        return $rows->map(function (Row $r) use ($context): Row {
            // @mago-ignore analysis:mixed-assignment
            $value = $this->function->eval($r, $context);
            $type = $this->entry instanceof Definition ? $this->entry->type() : null;

            if ($value instanceof ScalarResult) {
                $type = $value->type;
                // @mago-ignore analysis:mixed-assignment
                $value = $value->value;
            }

            return $r->set(
                $type
                    ? $context->entryFactory()->createAs($this->entryName(), $value, $type)
                    : $context->entryFactory()->create(
                        $this->entryName(),
                        $value,
                        $this->entry instanceof Definition ? $this->entry : null,
                    ),
            );
        });
    }

    private function entryName(): string
    {
        return $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;
    }
}
