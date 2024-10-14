<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final class FileFormatOption
{
    public function __construct(private readonly Path $filePath, private readonly string $inputFormatOption)
    {
    }

    public function get(InputInterface $input) : FileFormat
    {
        return FileFormat::from((new TypedOption($this->inputFormatOption))->asString($input, $this->filePath->extension() === false ? null : $this->filePath->extension()));
    }
}
