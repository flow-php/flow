<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Pipeline\SynchronousPipeline;

final class ETL
{
    private Extractor $extractor;

    private Pipeline $pipeline;

    private function __construct(Extractor $extractor, Pipeline $pipeline)
    {
        $this->extractor = $extractor;
        $this->pipeline = $pipeline;
    }

    public static function extract(Extractor $extractor, Pipeline $pipeline = null) : self
    {
        return new self($extractor, $pipeline ?? new SynchronousPipeline());
    }

    public function onError(ErrorHandler $handler) : self
    {
        $this->pipeline->onError($handler);

        return $this;
    }

    public function transform(Transformer $transformer) : self
    {
        $this->pipeline->registerTransformer($transformer);

        return $this;
    }

    public function load(Loader $loader) : self
    {
        $this->pipeline->registerLoader($loader);

        return $this;
    }

    /**
     * Keep extracting rows and passing them through all transformers up to this point.
     * From here all transformed Rows are collected and merged together before pushing them forward.
     */
    public function collect() : self
    {
        $this->pipeline = new CollectingPipeline($this->pipeline);

        return $this;
    }

    /**
     * Keep extracting rows and passing them through all transformers up to this point.
     * From here each transformed Row is divided and pushed forward to following pipeline elements.
     *
     * @throws Exception\InvalidArgumentException
     */
    public function parallelize(int $chunks) : self
    {
        $this->pipeline = new ParallelizingPipeline($this->pipeline, $chunks);

        return $this;
    }

    public function fetch(int $limit = 0) : Rows
    {
        if ($limit < 0) {
            throw new InvalidArgumentException("Fetch limit can't be lower than 0");
        }

        $rows = new Rows();
        $this->pipeline->process($this->extractor->extract(), function (Rows $nextRows) use ($limit, &$rows) : void {
            /** @var Rows $rows */
            if ($limit === 0) {
                $rows = $rows->merge($nextRows);
            }

            if ($limit > 0 && $rows->count() < $limit) {
                $rows = $rows->merge($nextRows);

                if ($rows->count() >= $limit) {
                    $rows = $rows->dropRight($rows->count() - $limit);
                }
            }
        });

        /** @var Rows $rows */
        return $rows;
    }

    /**
     * @param int $limit maximum numbers of rows to display
     * @param int $truncate if set to 0 columns are not truncated
     * @param null|Formatter $formatter
     *
     * @throws InvalidArgumentException
     *
     * @return string
     */
    public function display(int $limit = 20, int $truncate = 20, Formatter $formatter = null) : string
    {
        $formatter = $formatter ?? new AsciiTableFormatter();

        return $formatter->format($this->fetch($limit), $truncate);
    }

    public function run() : void
    {
        $this->pipeline->process($this->extractor->extract());
    }
}
