<?php declare(strict_types=1);

namespace Flow\ETL\DataFrame;

use Flow\ETL\DataFrame;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Formatter;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

final class PartitionedDataFrame
{
    public function __construct(private readonly DataFrame $df)
    {
    }

    public function display(int $limit = 20, int|bool $truncate = 20, Formatter $formatter = new AsciiTableFormatter()) : string
    {
        return $this->df->display($limit, $truncate, $formatter);
    }

    public function fetch(?int $limit = null) : Rows
    {
        return $this->df->fetch($limit);
    }

    public function get() : \Generator
    {
        return $this->df->get();
    }

    public function getAsArray() : \Generator
    {
        return $this->df->getAsArray();
    }

    public function getEach() : \Generator
    {
        return $this->df->getEach();
    }

    public function getEachAsArray() : \Generator
    {
        return $this->df->getEachAsArray();
    }

    public function load(Loader $loader) : DataFrame
    {
        return $this->write($loader);
    }

    public function mode(SaveMode $mode) : self
    {
        $this->df->mode($mode);

        return $this;
    }

    /**
     * @param null|callable(Rows $rows): void $callback
     */
    public function run(?callable $callback = null) : void
    {
        $this->df->run($callback);
    }

    public function write(Loader $loader) : DataFrame
    {
        return $this->df->write($loader);
    }
}
