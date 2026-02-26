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

echo 'Cast "pending": ' . type_enum(Status::class)->cast('pending')->name . "\n";
echo 'Cast "completed": ' . type_enum(Status::class)->cast('completed')->name . "\n";
echo 'Pass through: ' . type_enum(Status::class)->cast(Status::Active)->name . "\n";
