<?php

declare(strict_types=1);

namespace Flow\PostgreSql;

final class DeparseOptions
{
    private bool $commasStartOfLine = false;

    private int $indentSize = 4;

    private int $maxLineLength = 80;

    private bool $prettyPrint = true;

    private bool $trailingNewline = false;

    public static function new() : self
    {
        return new self();
    }

    public function commasAtStartOfLine() : bool
    {
        return $this->commasStartOfLine;
    }

    public function commasStartOfLine(bool $commasStartOfLine = true) : self
    {
        $this->commasStartOfLine = $commasStartOfLine;

        return $this;
    }

    public function getIndentSize() : int
    {
        return $this->indentSize;
    }

    public function getMaxLineLength() : int
    {
        return $this->maxLineLength;
    }

    public function hasPrettyPrint() : bool
    {
        return $this->prettyPrint;
    }

    public function hasTrailingNewline() : bool
    {
        return $this->trailingNewline;
    }

    public function indentSize(int $indentSize) : self
    {
        $this->indentSize = $indentSize;

        return $this;
    }

    public function maxLineLength(int $maxLineLength) : self
    {
        $this->maxLineLength = $maxLineLength;

        return $this;
    }

    public function prettyPrint(bool $prettyPrint = true) : self
    {
        $this->prettyPrint = $prettyPrint;

        return $this;
    }

    public function trailingNewline(bool $trailingNewline = true) : self
    {
        $this->trailingNewline = $trailingNewline;

        return $this;
    }
}
