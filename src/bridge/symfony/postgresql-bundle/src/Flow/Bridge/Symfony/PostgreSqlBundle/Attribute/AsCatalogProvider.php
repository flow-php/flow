<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Attribute;

#[\Attribute(\Attribute::TARGET_CLASS)]
final readonly class AsCatalogProvider
{
    public function __construct() {}
}
