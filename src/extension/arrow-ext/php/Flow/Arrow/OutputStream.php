<?php

declare(strict_types=1);

namespace Flow\Arrow;

interface OutputStream
{
    public function append(string $data) : self;
}
