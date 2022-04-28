<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\League;

use Flow\ETL\Loader;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use League\Csv\Writer;

/**
 * @implements Loader<array{
 *   path: string,
 *   open_mode: string,
 *   with_header: boolean,
 *   safe_mode: boolean,
 *   delimiter: string,
 *   enclosure: string, escape: string
 * }>
 */
final class CSVLoader implements Loader
{
    private ?Writer $writer = null;

    private bool $headerAdded;

    public function __construct(
        private string $path,
        private string $openMode = 'w+',
        private bool $withHeader = true,
        private bool $safeMode = true,
        private string $delimiter = ',',
        private string $enclosure = '"',
        private string $escape = '\\'
    ) {
        $this->headerAdded = false;
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'open_mode' => $this->openMode,
            'with_header' => $this->withHeader,
            'safe_mode' => $this->safeMode,
            'delimiter' => $this->delimiter,
            'enclosure' => $this->enclosure,
            'escape' => $this->escape,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->openMode = $data['open_mode'];
        $this->withHeader = $data['with_header'];
        $this->safeMode = $data['safe_mode'];
        $this->delimiter = $data['delimiter'];
        $this->escape = $data['escape'];
        $this->enclosure = $data['enclosure'];
        $this->headerAdded = false;
        $this->writer = null;
    }

    /**
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress InaccessibleProperty
     */
    public function load(Rows $rows) : void
    {
        if ($this->withHeader && !$this->headerAdded) {
            $this->writer()->insertOne($rows->first()->entries()->map(fn (Entry $entry) => $entry->name()));
            $this->headerAdded = true;
        }

        $this->writer()->insertAll($rows->toArray());
    }

    private function writer() : Writer
    {
        if ($this->writer === null) {
            $path = ($this->safeMode)
                ? (\rtrim($this->path, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . \uniqid() . '.csv')
                : $this->path;

            if ($this->safeMode) {
                \mkdir(\rtrim($this->path, DIRECTORY_SEPARATOR));
            }

            $this->writer = Writer::createFromPath($path, $this->openMode);
            $this->writer->setDelimiter($this->delimiter);
            $this->writer->setEnclosure($this->enclosure);
            $this->writer->setEscape($this->escape);
        }

        return $this->writer;
    }
}
