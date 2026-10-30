<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use Flow\ETL\Adapter\JSON\IO\Extractor\JsonExtractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

use function Flow\CLI\option_bool;
use function Flow\CLI\option_string_nullable;

final readonly class JsonExtractorFactory
{
    public function __construct(
        private Path $path,
        private string $pointerOption = 'input-json-pointer',
        private string $pointerAsColumnNameOption = 'input-json-pointer-column-name',
    ) {}

    public function get(InputInterface $input): JsonExtractor
    {
        $extractor = new JsonExtractor($this->path);

        $pointer = option_string_nullable($this->pointerOption, $input);

        if ($pointer !== null) {
            $extractor->withPointer($pointer, option_bool($this->pointerAsColumnNameOption, $input));
        }

        return $extractor;
    }
}
