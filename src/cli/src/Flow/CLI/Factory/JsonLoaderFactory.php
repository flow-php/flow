<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\CLI\{option_bool_nullable, option_string_nullable};
use function Flow\ETL\Adapter\JSON\to_json;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class JsonLoaderFactory
{
    public function __construct(
        private Path $path,
        private string $dateTimeFormat = 'output-json-date-time-format',
        private string $putRowsInNewLine = 'output-json-rows-in-new-line',
    ) {
    }

    public function get(InputInterface $input) : JsonLoader
    {
        $extractor = to_json($this->path);

        $dateTimeFormat = option_string_nullable($this->dateTimeFormat, $input);
        $putRowsInNewLine = option_bool_nullable($this->putRowsInNewLine, $input);

        if ($dateTimeFormat !== null) {
            $extractor->withDateTimeFormat($dateTimeFormat);
        }

        if ($putRowsInNewLine !== null) {
            $extractor->withRowsInNewLines($putRowsInNewLine);
        }

        return $extractor;
    }
}
