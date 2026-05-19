<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Throwable;

final readonly class HitsIntoRowsTransformer implements Transformer
{
    public function __construct(
        private DocumentDataSource $source = DocumentDataSource::source,
    ) {}

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $context->telemetry()->transformationStarted($this, []);

        try {
            $newRows = [];

            foreach ($rows as $row) {
                if (!$row->has('hits')) {
                    continue;
                }

                /**
                 * @var array{hits: array<array{_source: array<string, mixed>, fields: array<string, mixed>}>} $hits
                 */
                $hits = $row->get('hits')->value();

                foreach ($hits['hits'] as $hit) {
                    $entries = [];

                    $source = match ($this->source) {
                        DocumentDataSource::source => '_source',
                        DocumentDataSource::fields => 'fields',
                    };

                    /**
                     * @var string $key
                     * @var mixed $value
                     */
                    foreach ($hit[$source] as $key => $value) {
                        $entries[] = $context->entryFactory()->create($key, $value);
                    }

                    $newRows[] = Row::create(...$entries);
                }
            }

            $result = new Rows(...$newRows);

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
