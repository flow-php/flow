<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\API\Config\ConfigBuilder;
use Flow\ETL\API\DataFrame;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\RowsExtractor;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Values\Rows;

final readonly class Flow
{
    private Config $config;

    public function __construct(Config|ConfigBuilder|null $config = null)
    {
        if ($config instanceof ConfigBuilder) {
            $config = $config->build();
        }

        $this->config = $config ?: (new ConfigBuilder())->build();
    }

    public static function setUp(ConfigBuilder|Config $config): self
    {
        return new self($config instanceof ConfigBuilder ? $config->build() : $config);
    }

    public function process(Rows ...$rows): DataFrame
    {
        return new DataFrame(new RowsExtractor(...$rows), $this->config);
    }

    public function read(Extractor $extractor): DataFrame
    {
        return new DataFrame($extractor, $this->config);
    }
}
