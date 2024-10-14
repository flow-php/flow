<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use Flow\CLI\Options\TypedOption;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final class JsonExtractorFactory
{
    public function __construct(
        private readonly Path $path,
        private readonly string $pointerOption = 'json-pointer',
        private readonly string $pointerAsEntryNameOption = 'json-pointer-entry-name',
    ) {
    }

    public function get(InputInterface $input) : JsonExtractor
    {
        $extractor = new JsonExtractor($this->path);

        $pointer = (new TypedOption($this->pointerOption))->asStringNullable($input);

        if ($pointer !== null) {
            $extractor->withPointer($pointer, (new TypedOption($this->pointerAsEntryNameOption))->asBool($input));
        }

        return $extractor;
    }
}
