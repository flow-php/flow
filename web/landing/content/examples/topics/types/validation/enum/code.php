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

echo 'Is Status::Active valid? ' . (type_enum(Status::class)->isValid(Status::Active) ? 'yes' : 'no') . "\n";
echo 'Is "active" valid? ' . (type_enum(Status::class)->isValid('active') ? 'yes' : 'no') . "\n";
echo 'Is "invalid" valid? ' . (type_enum(Status::class)->isValid('invalid') ? 'yes' : 'no') . "\n";
