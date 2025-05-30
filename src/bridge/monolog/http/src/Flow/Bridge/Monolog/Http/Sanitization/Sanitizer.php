<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http\Sanitization;

interface Sanitizer
{
    /**
     * Return an array representation of the sanitizer.
     *
     * @return array<string, mixed>
     */
    public function normalize() : array;

    /**
     * Sanitize a string value.
     */
    public function sanitize(string $value) : string;
}
