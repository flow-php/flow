<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{
 *     path: Path,
 *     safe_mode: boolean,
 *     new_line_separator: string
 *  }>
 */
final class TextLoader implements Closure, Loader
{
    private ?FileStream $fileStream;

    public function __construct(
        private readonly Path $path,
        private readonly bool $safeMode = false,
        private string $newLineSeparator = PHP_EOL,
    ) {
        $this->fileStream = null;
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'safe_mode' => $this->safeMode,
            'new_line_separator' => $this->newLineSeparator,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->safeMode = $data['safe_mode'];
        $this->newLineSeparator = $data['new_line_separator'];
        $this->fileStream = null;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (\count($context->partitionEntries())) {
            throw new RuntimeException('Partitioning is not supported yet');
        }

        foreach ($rows as $row) {
            if ($row->entries()->count() > 1) {
                throw new RuntimeException(\sprintf('Text data loader supports only a single entry rows, and you have %d rows.', $row->entries()->count()));
            }

            \fwrite($this->stream($context)->resource(), $row->entries()->all()[0]->toString() . $this->newLineSeparator);
        }
    }

    /**
     * @psalm-suppress InvalidPropertyAssignmentValue
     */
    public function closure(Rows $rows, FlowContext $context) : void
    {
        if ($this->fileStream !== null && $this->fileStream->isOpen()) {
            $this->fileStream->close();
        }
    }

    /**
     * @throws RuntimeException
     *
     * @psalm-suppress InvalidNullableReturnType
     */
    private function stream(FlowContext $context) : FileStream
    {
        if ($this->fileStream === null) {
            $this->fileStream = $context->fs()->open(
                $this->safeMode ? $this->path->randomize() : $this->path,
                Mode::WRITE
            );
        }

        return $this->fileStream;
    }
}
