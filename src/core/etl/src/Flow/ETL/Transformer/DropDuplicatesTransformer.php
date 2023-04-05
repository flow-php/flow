<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\DropDuplicates\Hashes;

/**
 * @implements Transformer<array>
 */
final class DropDuplicatesTransformer implements Transformer
{
    private Hashes $deduplication;

    /**
     * @var array<EntryReference|string>
     */
    private array $entries;

    public function __construct(string|EntryReference ...$entries)
    {
        if (!\count($entries)) {
            throw new InvalidArgumentException('DropDuplicatesTransformer requires at least one entry');
        }

        $this->entries = $entries;
        $this->deduplication = new Hashes();
    }

    /**
     * @throws RuntimeException
     */
    public function __serialize() : array
    {
        throw new RuntimeException('DropDuplicates is not serializable');
    }

    /**
     * @param array $data
     *
     * @throws RuntimeException
     */
    public function __unserialize(array $data) : void
    {
        throw new RuntimeException('DropDuplicates is not serializable');
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $newRows = [];

        foreach ($rows as $row) {
            /** @var array<mixed> $values */
            $values = [];

            foreach ($this->entries as $entry) {
                try {
                    /** @psalm-suppress MixedAssignment */
                    $values[] = $row->valueOf($entry);
                } catch (InvalidArgumentException $e) {
                    $values[] = null;
                }
            }

            $hash = \md5(\serialize($values));

            if (!$this->deduplication->exists($hash)) {
                $newRows[] = $row;
                $this->deduplication->add($hash);
            }
        }

        return new Rows(...$newRows);
    }
}
