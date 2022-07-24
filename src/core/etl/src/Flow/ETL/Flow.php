<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\Pipeline\SynchronousPipeline;

final class Flow
{
    public function __construct(private readonly ConfigBuilder $configBuilder = new ConfigBuilder())
    {
    }

    public static function setUp(ConfigBuilder $configBuilder) : self
    {
        return new self($configBuilder);
    }

    public function extract(Extractor $extractor) : DataFrame
    {
        return new DataFrame(
            (new SynchronousPipeline())->source($extractor),
            $this
                ->configBuilder
                ->build()
        );
    }

    public function process(Rows $rows) : DataFrame
    {
        return new DataFrame(
            (new SynchronousPipeline())->source(new ProcessExtractor($rows)),
            $this
                ->configBuilder
                ->build()
        );
    }

    /**
     * Alias for Flow::extract function.
     */
    public function read(Extractor $extractor) : DataFrame
    {
        return self::extract($extractor);
    }
}
