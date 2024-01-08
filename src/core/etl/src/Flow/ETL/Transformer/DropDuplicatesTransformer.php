<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\DropDuplicates\Hashes;

final class DropDuplicatesTransformer implements Transformer
{
    private Hashes $deduplication;

    /**
     * @var array<Reference|string>
     */
    private array $entries;

    public function __construct(string|Reference ...$entries)
    {
        if ([] === $entries) {
            throw new InvalidArgumentException('DropDuplicatesTransformer requires at least one entry');
        }

        $this->entries = $entries;
        $this->deduplication = new Hashes();
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $newRows = [];

        foreach ($rows as $row) {
            $values = [];

            foreach ($this->entries as $entry) {
                try {
                    $values[] = $row->valueOf($entry);
                } catch (InvalidArgumentException) {
                    $values[] = null;
                }
            }

            $hash = \hash('xxh128', \serialize($values));

            if (!$this->deduplication->exists($hash)) {
                $newRows[] = $row;
                $this->deduplication->add($hash);
            }
        }

        return new Rows(...$newRows);
    }
}
