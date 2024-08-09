function when(mixed $condition, mixed $then, mixed $else = null) : When
{
    return new When($condition, $then, $else);
}