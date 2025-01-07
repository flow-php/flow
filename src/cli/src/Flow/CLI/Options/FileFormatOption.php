<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use function Flow\CLI\option_string;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class FileFormatOption
{
    public function __construct(private Path $filePath, private string $inputFormatOption)
    {
    }

    public function get(InputInterface $input) : FileFormat
    {
        return FileFormat::from(option_string($this->inputFormatOption, $input, $this->filePath->extension() === false ? null : $this->filePath->extension()));
    }
}
