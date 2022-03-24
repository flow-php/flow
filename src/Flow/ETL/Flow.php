<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Extractor\ProcessExtractor;

final class Flow
{
    private ConfigBuilder $configBuilder;

    public function __construct(ConfigBuilder $configBuilder = null)
    {
        $this->configBuilder = $configBuilder ?? new ConfigBuilder();
    }

    /**
     * @param ConfigBuilder $configBuilder
     *
     * @return self
     */
    public static function setUp(ConfigBuilder $configBuilder) : self
    {
        return new self($configBuilder);
    }

    /**
     * @param Extractor $extractor
     *
     * @return DataFrame
     */
    public function extract(Extractor $extractor) : DataFrame
    {
        return new DataFrame(
            $extractor,
            $this->configBuilder->build()
        );
    }

    /**
     * @param Rows $rows
     *
     * @return DataFrame
     */
    public function process(Rows $rows) : DataFrame
    {
        return new DataFrame(
            new ProcessExtractor($rows),
            $this->configBuilder->build()
        );
    }

    /**
     * Alias for Flow::extract function.
     *
     * @param Extractor $extractor
     *
     * @return DataFrame
     */
    public function read(Extractor $extractor) : DataFrame
    {
        return self::extract($extractor);
    }
}
