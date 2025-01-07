<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\CLI\{option_bool_nullable, option_string_nullable};
use function Flow\ETL\Adapter\CSV\to_csv;
use Flow\ETL\Adapter\CSV\CSVLoader;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class CSVLoaderFactory
{
    public function __construct(
        private Path $path,
        private string $withHeaderOption = 'output-csv-header',
        private string $separatorOption = 'output-csv-separator',
        private string $enclosureOption = 'output-csv-enclosure',
        private string $escapeOption = 'output-csv-escape',
        private string $newLineSeparatorOption = 'output-csv-new-line-separator',
        private string $dateTimeFormat = 'output-csv-date-time-format',
    ) {
    }

    public function get(InputInterface $input) : CSVLoader
    {
        $extractor = to_csv($this->path);

        $withHeader = option_bool_nullable($this->withHeaderOption, $input);
        $separator = option_string_nullable($this->separatorOption, $input);
        $enclosure = option_string_nullable($this->enclosureOption, $input);
        $escape = option_string_nullable($this->escapeOption, $input);
        $newLineSeparator = option_string_nullable($this->newLineSeparatorOption, $input);
        $dateTimeFormat = option_string_nullable($this->dateTimeFormat, $input);

        if ($withHeader !== null) {
            $extractor->withHeader($withHeader);
        }

        if ($newLineSeparator !== null) {
            $extractor->withNewLineSeparator($newLineSeparator);
        }

        if ($dateTimeFormat !== null) {
            $extractor->withDateTimeFormat($dateTimeFormat);
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
