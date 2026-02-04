<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\{FlowContext, Row, Rows, Transformer, Transformer\Rename\RenameReplaceEntryStrategy};

/**
 * @deprecated Use `DataFrame::renameEach()` and `RenameReplaceStrategy`
 */
final readonly class RenameStrReplaceAllEntriesTransformer implements Transformer
{
    private RenameReplaceEntryStrategy $transformer;

    public function __construct(
        private string $search,
        private string $replace,
    ) {
        $this->transformer = new RenameReplaceEntryStrategy($this->search, $this->replace);
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
