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
        $index = 0;

        while ($generator->valid()) {
            /** @var Rows $rows */
            $rows = $generator->current();
            $generator->next();

            if ($index === 0) {
                $rows = $rows->makeFirst();
            }

            if ($generator->valid() === false) {
                $rows = $rows->makeLast();
            }

            foreach ($this->elements as $element) {
                $rows = $element->process($rows);
            }

            $index++;
        }
    }
}
