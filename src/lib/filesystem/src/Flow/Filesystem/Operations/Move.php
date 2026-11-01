<?php

declare(strict_types=1);

namespace Flow\Filesystem\Operations;

use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\file_copy;

/**
 * Cross-filesystem moves are not atomic (copy then remove); re-running is idempotent.
 */
final readonly class Move
{
    public function __construct(
        private FilesystemTable $table,
        private OperationOptions $options = new OperationOptions(),
    ) {}

    public function execute(Path $from, Path $to): bool
    {
        $sourceFs = $this->table->for($from);
        $destFs = $this->table->for($to);

        if ($sourceFs === $destFs) {
            return $sourceFs->mv($from, $to);
        }

        file_copy($this->table, $this->options)->execute($from, $to);

        return $sourceFs->rm($from);
    }
}
