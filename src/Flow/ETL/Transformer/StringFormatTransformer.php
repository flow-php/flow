<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{entry_name:string,format:string}>
 * @psalm-immutable
 */
final class StringFormatTransformer implements Transformer
{
    private string $entryName;

    private string $format;

    public function __construct(string $entryName, string $format)
    {
        $this->entryName = $entryName;
        $this->format = $format;
    }

    public function __serialize() : array
    {
        return [
            'entry_name' => $this->entryName,
            'format' => $this->format,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryName = $data['entry_name'];
        $this->format = $data['format'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            $entry = $row->get($this->entryName);

            return $row->set(
                new Row\Entry\StringEntry($entry->name(), \sprintf($this->format, $entry->toString()))
            );
        };

        return $rows->map($transformer);
    }
}
