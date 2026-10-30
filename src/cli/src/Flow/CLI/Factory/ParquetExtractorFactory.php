<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetExtractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

use function count;
use function Flow\CLI\option_list_of_strings;
use function Flow\ETL\Adapter\Parquet\from_parquet;

final readonly class ParquetExtractorFactory
{
    public function __construct(
        private Path $path,
        private string $columns = 'input-parquet-columns',
    ) {}

    public function get(InputInterface $input): ParquetExtractor
    {
        $extractor = from_parquet($this->path);

        $columns = option_list_of_strings($this->columns, $input);

        if (count($columns)) {
            $extractor->withColumns($columns);
        }

        return $extractor;
    }
}
