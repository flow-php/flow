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

echo 'From string: ' . type_enum(Status::class)->assert('active')->name . "\n";
echo 'From enum: ' . type_enum(Status::class)->assert(Status::Pending)->name . "\n";
