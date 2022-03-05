<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{entry_renames: array<Transformer\Rename\EntryRename>}>
 * @psalm-immutable
 */
final class RenameEntriesTransformer implements Transformer
{
    /**
     * @var Rename\EntryRename[]
     */
    private array $entryRenames;

    public function __construct(Transformer\Rename\EntryRename ...$entryRenames)
    {
        $this->entryRenames = $entryRenames;
    }

    public function __serialize() : array
    {
        return [
            'entry_renames' => $this->entryRenames,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryRenames = $data['entry_renames'];
    }

    public function transform(Rows $rows) : Rows
    {
        foreach ($this->entryRenames as $entryRename) {
            $rows = $rows->map(function (Row $row) use ($entryRename) : Row {
                return $row->rename($entryRename->from(), $entryRename->to());
            });
        }

        return $rows;
    }
}
