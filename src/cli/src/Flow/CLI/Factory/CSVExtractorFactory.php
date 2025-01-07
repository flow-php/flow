<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\CLI\{option_bool_nullable, option_string_nullable};
use function Flow\ETL\Adapter\CSV\from_csv;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class CSVExtractorFactory
{
    public function __construct(
        private Path $path,
        private string $withHeaderOption = 'input-csv-header',
        private string $emptyToNullOption = 'input-csv-empty-to-null',
        private string $separatorOption = 'input-csv-separator',
        private string $enclosureOption = 'input-csv-enclosure',
        private string $escapeOption = 'input-csv-escape',
    ) {
    }

    public function get(InputInterface $input) : CSVExtractor
    {
        $extractor = from_csv($this->path);

        $withHeader = option_bool_nullable($this->withHeaderOption, $input);
        $emptyToNull = option_bool_nullable($this->emptyToNullOption, $input);
        $separator = option_string_nullable($this->separatorOption, $input);
        $enclosure = option_string_nullable($this->enclosureOption, $input);
        $escape = option_string_nullable($this->escapeOption, $input);

        if ($withHeader !== null) {
            $extractor->withHeader($withHeader);
        }

        if ($emptyToNull !== null) {
            $extractor->withEmptyToNull($emptyToNull);
        }

        if ($separator !== null) {
            $extractor->withSeparator($separator);
        }

        if ($enclosure !== null) {
            $extractor->withEnclosure($enclosure);
        }

        if ($escape !== null) {
            $extractor->withEscape($escape);
        }

        return $extractor;
    }
}
