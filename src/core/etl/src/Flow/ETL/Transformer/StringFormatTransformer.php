<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{ref: References, format:string}>
 */
final class StringFormatTransformer implements Transformer
{
    private readonly References $refs;

    public function __construct(
        string|Reference $entry,
        private readonly string $format
    ) {
        $this->refs = References::init($entry);
    }

    public function __serialize() : array
    {
        return [
            'ref' => $this->refs,
            'format' => $this->format,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->refs = $data['ref'];
        $this->format = $data['format'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /** @var EntryReference $ref */
        foreach ($this->refs as $ref) {
            $transformer = function (Row $row) use ($ref) : Row {
                $entry = $row->get($ref);

                return $row->set(
                    new Row\Entry\StringEntry($entry->name(), \sprintf($this->format, $entry->toString()))
                );
            };

            $rows = $rows->map($transformer);
        }

        return $rows;
    }
}
