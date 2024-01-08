<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class AddStampToStringEntryTransformer implements Transformer
{
    public function __construct(private string $entryName, private string $stamp, private string $divider)
    {
    }

    public static function divideBySemicolon(string $entryName, string $stamp) : self
    {
        return new self($entryName, $stamp, ':');
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(
            fn (Row $row) : Row => $row->set(
                new StringEntry(
                    $this->entryName,
                    \sprintf('%s%s%s', $row->get($this->entryName)->value(), $this->divider, $this->stamp)
                )
            )
        );
    }
}
