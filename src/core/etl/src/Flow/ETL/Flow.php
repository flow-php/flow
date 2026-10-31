<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\API\Config\ConfigBuilder;
use Flow\ETL\API\DataFrame;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\RowsExtractor;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Writes;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Runtime\FlowContext;
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
        return $this->read(new RowsExtractor(...$rows));
    }

    public function read(Extractor $extractor): DataFrame
    {
        return new DataFrame(new Read($extractor), new Writes(), new FlowContext($this->config));
    }
}
