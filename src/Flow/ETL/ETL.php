<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Extractor\ProcessExtractor;

/**
 * @codeCoverageIgnore
 *
 * @deprecated please use Flow\ETL\Flow instead
 */
final class ETL
{
    /**
     * @param Extractor $extractor
     * @param null|Config $configuration
     *
     * @return DataFrame
     */
    public static function extract(Extractor $extractor, Config $configuration = null) : DataFrame
    {
        return new DataFrame(
            $extractor,
            $configuration ?? Config::default()
        );
    }

    /**
     * @param Rows $rows
     * @param null|Config $configuration
     *
     * @return DataFrame
     */
    public static function process(Rows $rows, Config $configuration = null) : DataFrame
    {
        return new DataFrame(
            new ProcessExtractor($rows),
            $configuration ?? Config::default()
        );
    }

    /**
     * Alias for ETL::extract function.
     *
     * @param Extractor $extractor
     * @param null|Config $configuration
     *
     * @return DataFrame
     */
    public static function read(Extractor $extractor, Config $configuration = null) : DataFrame
    {
        return self::extract($extractor, $configuration);
    }
}
