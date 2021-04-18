<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\Element;

final class SynchronousPipeline implements Pipeline
{
    /**
     * @var array<Element>
     */
    private array $elements;

    public function __construct()
    {
        $this->elements = [];
    }

    public function register(Element $element) : void
    {
        $this->elements[] = $element;
    }

    /**
     * @param \Generator<int, Rows, mixed, void> $generator
     */
    public function process(\Generator $generator) : void
    {
        foreach ($generator as $rows) {
            foreach ($this->elements as $element) {
                $rows = $element->process($rows);
            }
        }
    }
}
