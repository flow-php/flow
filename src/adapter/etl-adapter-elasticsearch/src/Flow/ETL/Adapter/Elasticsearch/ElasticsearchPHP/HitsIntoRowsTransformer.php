<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{entry_factory: EntryFactory}>
 */
final class HitsIntoRowsTransformer implements Transformer
{
    public function __construct(private readonly EntryFactory $entryFactory = new NativeEntryFactory())
    {
    }

    public function __serialize() : array
    {
        return [
            'entry_factory' => $this->entryFactory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $newRows = [];

        foreach ($rows as $row) {
            if (!$row->has('hits')) {
                continue;
            }

            /**
             * @var array{hits: array<array{_source: array<string, mixed>}>} $hits
             */
            $hits = $row->get('hits')->value();

            foreach ($hits['hits'] as $hit) {
                $entries = [];

                /**
                 * @var mixed $value
                 */
                foreach ($hit['_source'] as $key => $value) {
                    $entries[] = $this->entryFactory->create($key, $value);
                }

                $newRows[] = Row::create(...$entries);
            }
        }

        return new Rows(...$newRows);
    }
}
