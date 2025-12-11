<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Copy;

interface CopyToOptionsStep extends CopyToFinalStep
{
    public function delimiter(string $delimiter) : self;

    public function encoding(string $encoding) : self;

    public function escape(string $escape) : self;

    public function forceQuote(string ...$columns) : self;

    public function forceQuoteAll() : self;

    public function format(CopyFormat $format) : self;

    public function nullAs(string $nullString) : self;

    public function quote(string $quote) : self;

    public function withHeader(bool $header = true) : self;
}
