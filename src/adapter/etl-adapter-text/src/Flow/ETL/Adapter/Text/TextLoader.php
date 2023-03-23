<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{
 *     path: Path,
 *     new_line_separator: string
 *  }>
 */
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

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'new_line_separator' => $this->newLineSeparator,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->newLineSeparator = $data['new_line_separator'];
    }

    /**
     * @psalm-suppress InvalidPropertyAssignmentValue
     */
    public function closure(Rows $rows, FlowContext $context) : void
    {
        $context->streams()->close($this->path);
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if ($context->partitionEntries()->count()) {
            foreach ($rows->partitionBy(...$context->partitionEntries()->all()) as $partition) {
                foreach ($partition->rows as $row) {
                    if ($row->entries()->count() > 1) {
                        throw new RuntimeException(\sprintf('Text data loader supports only a single entry rows, and you have %d rows.', $row->entries()->count()));
                    }

                    \fwrite(
                        $context->streams()->open($this->path, 'text', Mode::WRITE, $context->threadSafe(), $partition->partitions)->resource(),
                        $row->entries()->all()[0]->toString() . $this->newLineSeparator
                    );
                }
            }
        } else {
            foreach ($rows as $row) {
                if ($row->entries()->count() > 1) {
                    throw new RuntimeException(\sprintf('Text data loader supports only a single entry rows, and you have %d rows.', $row->entries()->count()));
                }

                \fwrite(
                    $context->streams()->open($this->path, 'text', Mode::WRITE, $context->threadSafe(), [])->resource(),
                    $row->entries()->all()[0]->toString() . $this->newLineSeparator
                );
            }
        }
    }
}
