<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
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

    /**
     * @return array{entry_renames: array<Transformer\Rename\EntryRename>}
     */
    public function __serialize() : array
    {
        return [
            'entry_renames' => $this->entryRenames,
        ];
    }

    /**
     * @param array{entry_renames: array<Transformer\Rename\EntryRename>} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->entryRenames = $data['entry_renames'];
    }

    public function transform(Rows $rows) : Rows
    {
        foreach ($this->entryRenames as $entryRename) {
            $rows = $rows->map(function (Row $row) use ($entryRename) : Row {
                return $row->rename_entry($entryRename->from(), $entryRename->to());
            });
        }

        return $rows;
    }
}
