<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_enum;

require __DIR__ . '/vendor/autoload.php';

enum Status: string
{
    case Pending = 'pending';
    case Active = 'active';
    case Completed = 'completed';
}

echo 'Active: ' . type_enum(Status::class)->assert(Status::Active)->name . "\n";
echo 'Pending: ' . type_enum(Status::class)->assert(Status::Pending)->name . "\n";
