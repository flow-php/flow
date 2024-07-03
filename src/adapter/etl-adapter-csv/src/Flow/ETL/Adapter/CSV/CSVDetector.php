<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Adapter\CSV\Detector\{Option, Options};
use Flow\ETL\Adapter\CSV\Exception\CantDetectCSVOptions;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\SourceStream;

final class CSVDetector
{
    private ?Option $fallback;

    private Options $options;

    private SourceStream $stream;

    public function __construct(SourceStream $stream, ?Option $fallback = new Option(',', '"', '\\'), ?Options $options = null)
    {
        $this->stream = $stream;
        $this->options = $options ?? Options::all();
        $this->fallback = $fallback;
    }

    /**
     * @throws CantDetectCSVOptions|InvalidArgumentException
     */
    public function detect(int $lines = 5) : Option
    {
        if ($lines < 1) {
            throw new InvalidArgumentException('Lines must be greater than 0');
        }

        $readLines = 1;

        foreach ($this->stream->readLines() as $line) {
            $this->options->parse($line);

            if ($readLines++ >= $lines) {
                break;
            }
        }

        try {
            $bestOption = $this->options->onlyValid()->best();
        } catch (CantDetectCSVOptions $e) {
            if ($this->fallback) {
                return $this->fallback;
            }

            throw $e;
        }

        $this->options = $this->options->reset();

        return $bestOption;
    }
}
