<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Copy;

interface CopyFromOptionsStep extends CopyFromFinalStep
{
    public function delimiter(string $delimiter) : self;

    public function encoding(string $encoding) : self;

    public function escape(string $escape) : self;

    public function forceNotNull(string ...$columns) : self;

    public function forceNull(string ...$columns) : self;

    public function format(CopyFormat $format) : self;

    public function nullAs(string $nullString) : self;

    public function onError(CopyOnError $behavior) : self;

    public function quote(string $quote) : self;

    public function withHeader(bool $header = true) : self;
}
