<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Extractor\RowsExtractor;
use Flow\ETL\Pipeline\SynchronousPipeline;

final readonly class Flow
{
    private Config $config;

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
            (new SynchronousPipeline($extractor)),
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
            (new SynchronousPipeline(new RowsExtractor(...$rows))),
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
