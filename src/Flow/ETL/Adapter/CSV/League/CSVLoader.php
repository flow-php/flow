<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\League;

use Flow\ETL\Loader;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use League\Csv\Writer;
use Ramsey\Uuid\Uuid;

final class CSVLoader implements Loader
{
    private string $path;

    private string $openMode;

    private bool $withHeader;

    private ?Writer $writer = null;

    private bool $headerAdded;

    private bool $safeMode;

    public function __construct(string $path, string $openMode = 'w+', bool $withHeader = true, bool $safeMode = true)
    {
        $this->headerAdded = false;
        $this->path = $path;
        $this->openMode = $openMode;
        $this->withHeader = $withHeader;
        $this->safeMode = $safeMode;
    }

    /**
     * @return array{
     *  path: string,
     *  open_mode: string,
     *  with_header: boolean,
     *  safe_mode: boolean
     * }
     */
    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'open_mode' => $this->openMode,
            'with_header' => $this->withHeader,
            'safe_mode' => $this->safeMode,
        ];
    }

    /**
     * @param array{
     *  path: string,
     *  open_mode: string,
     *  with_header: boolean,
     *  safe_mode: boolean
     * } $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->openMode = $data['open_mode'];
        $this->withHeader = $data['with_header'];
        $this->safeMode = $data['safe_mode'];
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
                ? (\rtrim($this->path, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . Uuid::uuid4()->toString() . '.csv')
                : $this->path;

            if ($this->safeMode) {
                \mkdir(\rtrim($this->path, DIRECTORY_SEPARATOR));
            }

            $this->writer = Writer::createFromPath($path, $this->openMode);
        }

        return $this->writer;
    }
}
