<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Row, Rows, String\StringStyles, Transformer, Transformer\Rename\RenameCaseEntryStrategy};

/**
 * @deprecated Use `DataFrame::renameEach()` and `RenameCaseTransformer`
 */
final class RenameAllCaseTransformer implements Transformer
{
    private RenameCaseEntryStrategy $transformer;

    public function __construct(
        bool $upper = false,
        bool $lower = false,
        bool $ucfirst = false,
        bool $ucwords = false,
    ) {
        if ($upper) {
            $this->transformer = new RenameCaseEntryStrategy(StringStyles::UPPER);
        }

        if ($lower) {
            $this->transformer = new RenameCaseEntryStrategy(StringStyles::LOWER);
        }

        if ($ucfirst) {
            $this->transformer = new RenameCaseEntryStrategy(StringStyles::UCFIRST);
        }

        if ($ucwords) {
            $this->transformer = new RenameCaseEntryStrategy(StringStyles::UCWORDS);
        }
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $context->telemetry()->transformationStarted($this);

        try {
            $result = $rows->map(function (Row $row) use ($context) : Row {
                foreach ($row->entries()->all() as $entry) {
                    $row = $this->transformer->rename($row, $entry, $context);
                }

                return $row;
            });

            $context->telemetry()->transformationCompleted($this, [
                TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS => $rows->count(),
                TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS => $result->count(),
            ]);

            return $result;
        } catch (\Throwable $e) {
            $context->telemetry()->transformationFailed($this, $e);

            throw $e;
        }
    }
}
