<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
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

    /**
     * @return array{entry_name: string, format: string}
     */
    public function __serialize() : array
    {
        return [
            'entry_name' => $this->entryName,
            'format' => $this->format,
        ];
    }

    /**
     * @param array{entry_name: string, format: string} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
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

            /** @psalm-suppress MixedArgument */
            return $row->set(
                /** @phpstan-ignore-next-line */
                new Row\Entry\StringEntry($entry->name(), \sprintf($this->format, $entry->value()))
            );
        };

        return $rows->map($transformer);
    }
}
