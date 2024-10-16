<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\CLI\{option_bool, option_string_nullable};
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final class JsonExtractorFactory
{
    public function __construct(
        private readonly Path $path,
        private readonly string $pointerOption = 'input-json-pointer',
        private readonly string $pointerAsEntryNameOption = 'input-json-pointer-entry-name',
    ) {
    }

    public function get(InputInterface $input) : JsonExtractor
    {
        $extractor = new JsonExtractor($this->path);

        $pointer = option_string_nullable($this->pointerOption, $input);

        if ($pointer !== null) {
            $extractor->withPointer($pointer, option_bool($this->pointerAsEntryNameOption, $input));
        }

        return $extractor;
    }
}
