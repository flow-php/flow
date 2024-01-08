<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;

final class TextLoader implements Closure, Loader, Loader\FileLoader
{
    public function __construct(
        private readonly Path $path,
        private string $newLineSeparator = PHP_EOL,
    ) {
        if ($this->path->isPattern()) {
            throw new \InvalidArgumentException("TextLoader path can't be pattern, given: " . $this->path->path());
        }
    }

    public function closure(FlowContext $context) : void
    {
        $context->streams()->close($this->path);
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if ($rows->partitions()->count()) {
            foreach ($rows as $row) {
                if ($row->entries()->count() > 1) {
                    throw new RuntimeException(\sprintf('Text data loader supports only a single entry rows, and you have %d rows.', $row->entries()->count()));
                }

                \fwrite(
                    $context->streams()->open($this->path, 'text', $context->appendSafe(), $rows->partitions()->toArray())->resource(),
                    $row->entries()->all()[0]->toString() . $this->newLineSeparator
                );
            }
        } else {
            foreach ($rows as $row) {
                if ($row->entries()->count() > 1) {
                    throw new RuntimeException(\sprintf('Text data loader supports only a single entry rows, and you have %d rows.', $row->entries()->count()));
                }

                \fwrite(
                    $context->streams()->open($this->path, 'text', $context->appendSafe(), [])->resource(),
                    $row->entries()->all()[0]->toString() . $this->newLineSeparator
                );
            }
        }
    }
}
