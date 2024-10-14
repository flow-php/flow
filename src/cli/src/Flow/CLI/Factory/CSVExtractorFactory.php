<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\CLI\{option_bool_nullable, option_string_nullable};
use function Flow\ETL\Adapter\CSV\from_csv;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final class CSVExtractorFactory
{
    public function __construct(
        private readonly Path $path,
        private readonly string $withHeaderOption = 'csv-header',
        private readonly string $emptyToNullOption = 'csv-empty-to-null',
        private readonly string $separatorOption = 'csv-separator',
        private readonly string $enclosureOption = 'csv-enclosure',
        private readonly string $escapeOption = 'csv-escape',
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
