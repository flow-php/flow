<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\CLI\{option_int_nullable, option_list_of_strings};
use function Flow\ETL\Adapter\Parquet\from_parquet;
use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class ParquetExtractorFactory
{
    public function __construct(
        private Path $path,
        private string $columns = 'input-parquet-columns',
        private string $offset = 'input-parquet-offset',
    ) {
    }

    public function get(InputInterface $input) : ParquetExtractor
    {
        $extractor = from_parquet($this->path);

        $columns = option_list_of_strings($this->columns, $input);
        $offset = option_int_nullable($this->offset, $input);

        if (\count($columns)) {
            $extractor->withColumns($columns);
        }

        if ($offset !== null) {
            $extractor->withOffset($offset);
        }

        return $extractor;
    }
}
