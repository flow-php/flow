<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final class HitsIntoRowsTransformer implements Transformer
{
    public function __construct(
        private readonly DocumentDataSource $source = DocumentDataSource::source,
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
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
                    DocumentDataSource::fields => 'fields'
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

        return new Rows(...$newRows);
    }
}
