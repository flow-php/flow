<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Detector;

use Flow\ETL\Adapter\CSV\Exception\CantDetectCSVOptions;

final class Options
{
    /**
     * @var array<Option>
     */
    private array $options;

    /**
     * @param array<Option> $options
     */
    public function __construct(array $options)
    {
        $this->options = $options;
    }

    public static function all() : self
    {
        $separators = [',', "\t", ';', '|', ' ', '_', '-', ':', '~', '@', '#', '$', '%', '^', '&', '*', '(', ')', '+', '=', '?', '!', '\\', '/', '.', '>', '<'];
        $enclosures = ['"', "'"];

        $options = [];

        foreach ($separators as $separator) {
            foreach ($enclosures as $enclosure) {
                $options[] = new Option($separator, $enclosure);
            }
        }

        return new self($options);
    }

    public function best() : Option
    {
        $best = null;

        foreach ($this->options as $option) {
            if ($best === null) {
                $best = $option;

                continue;
            }

            if ($option->score() > $best->score()) {
                $best = $option;
            }
        }

        if ($best === null) {
            throw new CantDetectCSVOptions('No best option found');
        }

        return $best;
    }

    public function onlyValid() : self
    {
        return new self(\array_filter($this->options, fn (Option $option) : bool => $option->isValid()));
    }

    public function parse(string $line) : void
    {
        foreach ($this->options as $option) {
            $option->parse($line);
        }
    }

    public function reset() : self
    {
        $options = [];

        foreach ($this->options as $option) {
            $options[] = $option->reset();
        }

        return new self($options);
    }
}
