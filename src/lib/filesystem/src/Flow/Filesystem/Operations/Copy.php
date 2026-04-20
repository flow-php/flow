<?php

declare(strict_types=1);

namespace Flow\Filesystem\Operations;

use Flow\Filesystem\{FilesystemTable, Path};

final readonly class Copy
{
    public function __construct(
        private FilesystemTable $table,
        private OperationOptions $options = new OperationOptions(),
    ) {
    }

    public function execute(Path $from, Path $to) : bool
    {
        $source = $this->table->for($from)->readFrom($from);
        $dest = $this->table->for($to)->writeTo($to);

        try {
            foreach ($source->iterate($this->options->chunkSize) as $chunk) {
                $dest->append($chunk);
            }
        } finally {
            $dest->close();
            $source->close();
        }

        return true;
    }
}
