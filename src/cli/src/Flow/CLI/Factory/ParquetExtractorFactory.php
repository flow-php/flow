<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use Flow\CLI\Options\TypedOption;
use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final class ParquetExtractorFactory
{
    public function __construct(
        private readonly Path $path,
        private readonly string $columns = 'parquet-columns',
        private readonly string $offset = 'parquet-offset',
    ) {
    }

    public function get(InputInterface $input) : ParquetExtractor
    {
        $extractor = from_parquet($this->path);

        $columns = (new TypedOption($this->columns))->asListOfStrings($input);
        $offset = (new TypedOption($this->offset))->asIntNullable($input);

        if (\count($columns)) {
            $extractor->withColumns($columns);
        }

        if ($offset !== null) {
            $extractor->withOffset($offset);
        }

        return $extractor;
    }
}
