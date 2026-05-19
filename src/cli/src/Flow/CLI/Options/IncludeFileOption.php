<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Symfony\Component\Console\Exception\InvalidArgumentException as SymfonyInvalidArgumentException;
use Symfony\Component\Console\Input\InputInterface;

use function Flow\CLI\option_string;
use function Flow\Filesystem\DSL\path_real;

/**
 * @template ExpectedClass
 */
final readonly class IncludeFileOption
{
    /**
     * @param string $filePathOptionName
     * @param class-string<ExpectedClass> $expectedClass
     */
    public function __construct(
        private string $filePathOptionName,
        private string $expectedClass,
    ) {
        if (!class_exists($this->expectedClass)) {
            throw new SymfonyInvalidArgumentException("Class {$this->expectedClass} does not exist.");
        }
    }

    /**
     * @return ExpectedClass
     */
    public function include(InputInterface $input)
    {
        $filePath = path_real(option_string($this->filePathOptionName, $input));

        $fs = new NativeLocalFilesystem();

        if ($fs->status($filePath) === null) {
            throw new SymfonyInvalidArgumentException("File '{$filePath->path()}' does not exist.");
        }

        $object = require $filePath->path();

        if (!$object instanceof $this->expectedClass) {
            throw new InvalidArgumentException(
                "File '{$filePath->path()}' does not return instance of '{$this->expectedClass}'.",
            );
        }

        return $object;
    }
}
