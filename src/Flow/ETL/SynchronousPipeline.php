<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Pipeline\Element;

final class SynchronousPipeline implements Pipeline
{
    /**
     * @var array<Element>
     */
    private array $elements;

    private ErrorHandler $errorHandler;

    public function __construct()
    {
        $this->elements = [];
        $this->errorHandler = new ThrowError();
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->errorHandler = $errorHandler;
    }

    public function register(Element $element) : void
    {
        $this->elements[] = $element;
    }

    /**
     * @param \Generator<int, Rows, mixed, void> $generator
     *
     * @throws \Throwable
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
                try {
                    $rows = $element->process($rows);
                } catch (\Throwable $exception) {
                    if ($this->errorHandler->throw($exception, $rows)) {
                        throw $exception;
                    }

                    if ($this->errorHandler->skipRows($exception, $rows)) {
                        break;
                    }
                }
            }

            $index++;
        }
    }
}
