<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Throwable;

use function array_key_last;
use function count;

final class ErrorHandlerSpy implements ErrorHandler
{
    /** @var array<\Throwable> */
    private array $errors = [];

    public function count(): int
    {
        return count($this->errors);
    }

    /**
     * @return array<\Throwable>
     */
    public function errors(): array
    {
        return $this->errors;
    }

    public function handle(Throwable $error): void
    {
        $this->errors[] = $error;
    }

    public function last(): ?Throwable
    {
        if ($this->errors === []) {
            return null;
        }

        return $this->errors[array_key_last($this->errors)];
    }
}
