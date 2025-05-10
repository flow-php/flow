<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\CLI\{option_bool_nullable, option_int_nullable, option_string_nullable};
use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use Flow\ETL\Adapter\Excel\{ExcelExtractor};
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class ExcelExtractorFactory
{
    public function __construct(
        private Path $path,
        private string $headerOption = 'input-excel-header',
        private string $sheetNameOption = 'input-excel-sheet-name',
        private string $offsetOption = 'input-excel-offset',
    ) {
    }

    public function get(InputInterface $input) : ExcelExtractor
    {
        $extractor = from_excel($this->path);

        $header = option_bool_nullable($this->headerOption, $input);
        $sheetName = option_string_nullable($this->sheetNameOption, $input);
        $offset = option_int_nullable($this->offsetOption, $input);

        if ($header !== null) {
            $extractor->withHeader($header);
        }

        if ($sheetName !== null) {
            $extractor->withSheetName($sheetName);
        }

        if ($offset !== null && $offset > 0) {
            $extractor->withOffset($offset);
        }

        return $extractor;
    }
}
