<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Rows;

final readonly class RowsNormalizer
{
    public function __construct(private EntryNormalizer $entryNormalizer, private string $rowNodeName = 'row')
    {
    }

    /**
     * @return \Generator<XMLNode>
     */
    public function normalize(Rows $rows) : \Generator
    {
        foreach ($rows as $row) {
            $node = XMLNode::nestedNode($this->rowNodeName);

            foreach ($row->entries() as $entry) {
                $node = $node->append($this->entryNormalizer->normalize($entry));
            }

            yield $node;
        }
    }
}
