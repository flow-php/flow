<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\Pipeline\SynchronousPipeline;

final class Flow
{
    private readonly Config $config;

    public function __construct(Config|ConfigBuilder|null $config = null)
    {
        if ($config instanceof ConfigBuilder) {
            $config = $config->build();
        }

        $this->config = $config ?: Config::default();
    }

    public static function setUp(ConfigBuilder|Config $config) : self
    {
        return new self($config instanceof ConfigBuilder ? $config->build() : $config);
    }

    public function extract(Extractor $extractor) : DataFrame
    {
        return new DataFrame(
            (new SynchronousPipeline())->setSource($extractor),
            $this->config
        );
    }

    public function from(Extractor $extractor) : DataFrame
    {
        return $this->read($extractor);
    }

    public function process(Rows ...$rows) : DataFrame
    {
        return new DataFrame(
            (new SynchronousPipeline())->setSource(new ProcessExtractor(...$rows)),
            $this->config
        );
    }

    /**
     * Alias for Flow::extract function.
     */
    public function read(Extractor $extractor) : DataFrame
    {
        return $this->extract($extractor);
    }
}
