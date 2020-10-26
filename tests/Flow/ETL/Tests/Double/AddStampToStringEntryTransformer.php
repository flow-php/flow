<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @psalm-immutable
 */
final class AddStampToStringEntryTransformer implements Transformer
{
    private string $entryName;

    private string $stamp;

    private string $divider;

    public function __construct(string $entryName, string $stamp, string $divider)
    {
        $this->entryName = $entryName;
        $this->stamp = $stamp;
        $this->divider = $divider;
    }

    public static function divideBySemicolon(string $entryName, string $stamp) : self
    {
        return new self($entryName, $stamp, ':');
    }

    public function transform(Rows $rows) : Rows
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
