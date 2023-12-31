<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\MeilisearchPHP;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class HitsIntoRowsTransformer implements Transformer
{
    public function __construct(
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $newRows = [];

        foreach ($rows as $row) {
            $entries = [];

            foreach ($row->toArray() as $key => $value) {
                $entries[] = $context->entryFactory()->create($key, $value);
            }

            $newRows[] = Row::create(...$entries);
        }

        return new Rows(...$newRows);
    }
}
