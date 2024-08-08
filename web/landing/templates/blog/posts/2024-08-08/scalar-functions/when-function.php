function when(ScalarFunction $ref, ScalarFunction $then, ?ScalarFunction $else = null) : When
{
    return new When($ref, $then, $else);
}