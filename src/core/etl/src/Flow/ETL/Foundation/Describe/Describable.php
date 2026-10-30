<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Describe;

use SensitiveParameter;

interface Describable
{
    /**
     * @param array<array-key, mixed> $data
     */
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static;

    /**
     * @return null|Description null when this instance cannot cross a process boundary (a live client, a custom collaborator)
     */
    public function describe(): ?Description;
}
