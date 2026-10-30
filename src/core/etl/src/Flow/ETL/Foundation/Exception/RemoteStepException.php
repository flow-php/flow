<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Exception;

use function sprintf;

final class RemoteStepException extends RuntimeException
{
    private function __construct(
        string $message,
        private readonly ?string $remoteClass,
    ) {
        parent::__construct($message);
    }

    /**
     * @param null|string $class the class of the exception the other process threw; null when it died without one
     * @param string $output what the other process wrote about the failure
     * @param string $where what failed there, for example "Split 3"
     */
    public static function of(?string $class, string $output, string $where): self
    {
        return new self(sprintf("%s failed in a shipped step:\n%s", $where, $output), $class);
    }

    public function remoteClass(): ?string
    {
        return $this->remoteClass;
    }
}
