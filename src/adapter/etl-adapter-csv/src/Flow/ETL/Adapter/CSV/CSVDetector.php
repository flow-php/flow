<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Adapter\CSV\Detector\Option;
use Flow\ETL\Adapter\CSV\Detector\Options;
use Flow\ETL\Adapter\CSV\Exception\CantDetectCSVOptions;
use Flow\ETL\Exception\InvalidArgumentException;

final class CSVDetector
{
    private ?Option $fallback;

    private Options $options;

    /**
     * @var resource
     */
    private $resource;

    private int $startingPosition;

    /**
     * @param resource $resource
     */
    public function __construct($resource, ?Option $fallback = new Option(',', '"', '\\'), ?Options $options = null)
    {
        /** @psalm-suppress DocblockTypeContradiction */
        if (!\is_resource($resource)) {
            throw new InvalidArgumentException('Argument must be a valid resource');
        }

        $this->resource = $resource;
        /** @phpstan-ignore-next-line */
        $this->startingPosition = \ftell($resource);
        $this->options = $options ?? Options::all();
        $this->fallback = $fallback;
    }

    public function __destruct()
    {
        \fseek($this->resource, $this->startingPosition);
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

        while ($line = \fgets($this->resource)) {
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
