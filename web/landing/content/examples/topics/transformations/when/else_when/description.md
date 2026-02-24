Using `when` with nested `else_when` conditions for multi-tier logic.

This example demonstrates how to nest multiple `when` functions to create complex conditional logic, similar to if-elseif-elseif-else chains in traditional programming.

The pattern evaluates conditions in order:
1. If score >= 90, grade is 'A'
2. Else if score >= 70, grade is 'B'
3. Else if score >= 50, grade is 'C'
4. Else grade is 'F'

This approach is useful for categorizing data into multiple levels or implementing grading systems, priority levels, or status determination based on multiple criteria.
